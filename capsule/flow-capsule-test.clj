;; flow-capsule-test: checks the capsule runner's own logic that runs no
;; capsule: where a run keeps its witnesses, and what it copies there
;; before the state root is deleted.
;;
;; Usage: bb flow-capsule-test.clj <path to flow-capsule.clj>
(require '[babashka.fs :as fs])

(load-file (first *command-line-args*))

(def failures (atom []))

(defn check [what ok]
  (println (str (if ok "pass" "FAIL") " | " what))
  (when-not ok (swap! failures conj what)))

;; Where witnesses go: FLOW_CAPSULE_WITNESS_DIR as given, else
;; $HOME/witnesses/<flow id>, the flow id FLOW_ID or else the name of the
;; calling flow's FLOW_DIRECTORY; nowhere is named without a home or a
;; flow id.
(check "FLOW_CAPSULE_WITNESS_DIR is taken as given"
       (= "/w/given" (flow-capsule/witness-root {"FLOW_CAPSULE_WITNESS_DIR" "/w/given"
                                                 "HOME" "/h/li"
                                                 "FLOW_ID" "622382"})))
(check "else the home's witnesses of the flow FLOW_ID names"
       (= "/h/li/witnesses/622382" (flow-capsule/witness-root {"HOME" "/h/li"
                                                               "FLOW_ID" "622382"
                                                               "FLOW_DIRECTORY" "/p/flows/622382"})))
(check "the home is read from HOME, not fixed"
       (= "/h/other/witnesses/622382" (flow-capsule/witness-root {"HOME" "/h/other"
                                                                  "FLOW_ID" "622382"})))
(check "FLOW_ID names the flow before FLOW_DIRECTORY"
       (= "/h/li/witnesses/622382" (flow-capsule/witness-root {"HOME" "/h/li"
                                                               "FLOW_ID" "622382"
                                                               "FLOW_DIRECTORY" "/p/flows/a68420"})))
(check "without FLOW_ID, the calling flow's directory names the flow"
       (= "/h/li/witnesses/622382" (flow-capsule/witness-root {"HOME" "/h/li"
                                                               "FLOW_DIRECTORY" "/p/flows/622382/"})))
(check "without a flow id, no witness root is named"
       (nil? (flow-capsule/witness-root {"HOME" "/h/li"})))
(check "without a home, no witness root is named"
       (nil? (flow-capsule/witness-root {"FLOW_ID" "622382" "FLOW_DIRECTORY" "/p/flows/622382"})))
(check "with neither, no witness root is named"
       (nil? (flow-capsule/witness-root {})))
(check "a run's directory is named by the flow version and a timestamp"
       (= "capsule-0.40.7-20261029T201500Z"
          (flow-capsule/witness-name "flow-nexus 0.40.7" (java.time.Instant/parse "2026-10-29T20:15:00.123Z"))))

;; A wake hands an asleep seat its whole queue as one vector datom, each
;; entry its own id, sender and priority, oldest first; the flow's first
;; turn holds it after the context it is composed into.
(let [turn (str "Purpose of the seat.\n\n"
                "[ { logical-delivery-00000000000000000007 Sender.{ Psyche test Secondary } Soft.Notice.«First of two. Tag w1.» }"
                " { logical-delivery-00000000000000000009 Flow Soft.Order.«Second of two. Tag w2.» } ]")]
  (check "a waking reads as one vector of its entries, oldest first"
         (= [{:id "logical-delivery-00000000000000000007" :sender "Sender Psyche test Secondary" :priority "Soft"}
             {:id "logical-delivery-00000000000000000009" :sender "Flow" :priority "Soft"}]
            (flow-capsule/waking-entries turn)))
  (check "a turn holding no waking reads no entries"
         (nil? (flow-capsule/waking-entries "Purpose of the seat. Tag w1.")))
  (check "two prompts of one entry each are not one vector of both"
         (= ["logical-delivery-00000000000000000007"]
            (map :id (flow-capsule/waking-entries
                      (str "[ { logical-delivery-00000000000000000007 Flow Soft.Notice.«a» } ]\n"
                           "[ { logical-delivery-00000000000000000009 Flow Soft.Notice.«b» } ]"))))))

;; The Nexus logs each admitted Task with its deadline in Unix seconds; a
;; reminder is witnessed against it to the second, within the stated
;; tolerance and never before it.
(let [log (str "flow-nexus: admitted logical-delivery-00000000000000000041 Order at 1793329020.010\n"
               "flow-nexus: admitted logical-delivery-00000000000000000042 Task minutes 1 deadline 1793329086 at 1793329026.123\n")]
  (check "a Task's logged deadline is read by its id"
         (= 1793329086 (flow-capsule/admitted-deadline log "logical-delivery-00000000000000000042")))
  (check "a request with no logged deadline reads none"
         (nil? (flow-capsule/admitted-deadline log "logical-delivery-00000000000000000041")))
  (check "a reminder's lag is its transcript stamp less the deadline, in seconds"
         (= 7 (flow-capsule/reminder-lag 1793329086 "2026-10-30T02:58:13.400Z"))))
(check "a reminder within the tolerance after its deadline is on time"
       (flow-capsule/on-time? 0))
(check "a reminder before its deadline is not on time"
       (not (flow-capsule/on-time? -1)))
(check "a reminder later than the tolerance is not on time"
       (not (flow-capsule/on-time? (inc flow-capsule/reminder-tolerance-seconds))))

;; What is kept: the raw Nexus log and every flow transcript, copied
;; whole, the originals left for teardown to delete.
(let [scratch (str (fs/create-temp-dir))
      state (str scratch "/state")
      log (str state "/nexus.log")
      claude (str scratch "/claude-project")
      codex (str state "/codex-home/sessions")
      root (str scratch "/witnesses")]
  (fs/create-dirs state)
  (fs/create-dirs claude)
  (fs/create-dirs (str codex "/2026/10/29"))
  (spit log "flow-nexus: started\n")
  (spit (str claude "/5e4de700.jsonl") "{\"type\":\"user\"}\n")
  (spit (str codex "/2026/10/29/rollout.jsonl") "{\"type\":\"turn\"}\n")
  (let [kept (flow-capsule/keep-witnesses! (str root "/capsule-0.40.7-20261029T201500Z")
                                           {"nexus.log" log
                                            "claude-transcripts" claude
                                            "codex-sessions" codex
                                            "absent" (str scratch "/absent")})]
    (check "the kept directory is the one named" (= kept (str root "/capsule-0.40.7-20261029T201500Z")))
    (check "the Nexus log is copied whole" (= "flow-nexus: started\n" (slurp (str kept "/nexus.log"))))
    (check "each Claude transcript is copied"
           (= "{\"type\":\"user\"}\n" (slurp (str kept "/claude-transcripts/5e4de700.jsonl"))))
    (check "each Codex session is copied, its tree kept"
           (= "{\"type\":\"turn\"}\n" (slurp (str kept "/codex-sessions/2026/10/29/rollout.jsonl"))))
    (check "a witness that does not exist is skipped" (not (fs/exists? (str kept "/absent"))))
    (check "the originals stay for teardown" (and (fs/exists? log) (fs/exists? (str claude "/5e4de700.jsonl")))))
  (fs/delete-tree scratch))

(if (seq @failures)
  (do (println (str (count @failures) " failed")) (System/exit 1))
  (println "all passed"))
