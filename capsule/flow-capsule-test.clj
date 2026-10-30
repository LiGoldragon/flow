;; flow-capsule-test: checks the capsule runner's own logic that runs no
;; capsule: where a run keeps its witnesses, and what it copies there
;; before the state root is deleted; and the takeover's readings: its
;; declarations, the ids and counters, the guard, the re-bind lines, the
;; seats read back and the window.
;;
;; Usage: bb flow-capsule-test.clj <path to flow-capsule.clj>
(require '[babashka.fs :as fs] '[clojure.string :as str])

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

;; ---------------------------------------------------------------- takeover
;; The bie3.6 takeover and rollback (procedure §6): an old Nexus is replaced
;; by a new one seeded past the old ids, its seats re-bound through Herdr,
;; and the old one restored by its recorded command.

;; The replay is split as the production replay splits it: at a newline
;; outside guillemets, so a Purpose may hold newlines.
(check "the replay splits at newlines outside guillemets, blank pieces dropped"
       (= ["Configure.Nexus.{ a }" "Configure.Metaflows.[ { «two\nlines» } ]"]
          (flow-capsule/split-replay "Configure.Nexus.{ a }\n\nConfigure.Metaflows.[ { «two\nlines» } ]\n")))

;; The takeover's declarations: every seat it keeps Awake is Maintained, a
;; Codex Secondary and an asleep OnDemand Primary are declared, and the old
;; Nexus is not given DefaultMinutes, which it does not know; the replay to
;; the new Nexus is the same declarations with it.
(let [lines ["Configure.Nexus.{ /src 60 }"
             "Configure.Models.[ { Secondary Psyche haiku } { Quaternary Mind luna } ]"
             "Configure.DefaultMinutes.30"
             "Configure.LeaveOuts.[ { { Mind test Quaternary } Codex [ CodexAgentsMd ] } ]"
             (str "Configure.Metaflows.[ { { Psyche test Secondary } [ spirit:core ] [] Simplified Explicit.[] Maintained «» [] }"
                  " { { Psyche test Tertiary } [ spirit:core ] [] Regular Default OnDemand «» [] }"
                  " { { Psyche test Quaternary } [ spirit:core ] [] Simplified Explicit.[] OnDemand «» [] }"
                  " { { Mind test Quaternary } [ spirit:core ] [] Simplified Explicit.[] OnDemand «» [] } ]")]
      {:keys [old replay]} (flow-capsule/takeover-configuration lines)
      metaflows (first (filter #(clojure.string/starts-with? % "Configure.Metaflows.") old))]
  (check "the old Nexus is not given DefaultMinutes"
         (not-any? #(clojure.string/starts-with? % "Configure.DefaultMinutes.") old))
  (check "the replay carries every old declaration and DefaultMinutes, in the file's order"
         (= (map #(subs % 0 14) replay)
            ["Configure.Nexu" "Configure.Mode" "Configure.Defa" "Configure.Leav" "Configure.Meta"]))
  (check "the replay's declarations equal the old ones"
         (= (remove #(clojure.string/starts-with? % "Configure.DefaultMinutes.") replay) old))
  (check "no kept seat is OnDemand but the asleep Primary"
         (= 1 (count (re-seq #"OnDemand" metaflows))))
  (check "the asleep Primary is declared OnDemand"
         (clojure.string/includes? metaflows "{ { Psyche test Primary } [ spirit:core intent:models ] [] Regular Default OnDemand «» [] }"))
  (check "a Codex Secondary is declared Maintained"
         (clojure.string/includes? metaflows "{ { Mind test Secondary } [ spirit:core intent:models ] [] Simplified Explicit.[] Maintained «» [] }"))
  (check "the Codex Secondary runs the Codex Quaternary's model"
         (clojure.string/includes? (first (filter #(clojure.string/starts-with? % "Configure.Models.") old))
                                   "{ Secondary Mind luna }"))
  (check "the Codex Secondary leaves out what the Codex Quaternary does"
         (clojure.string/includes? (first (filter #(clojure.string/starts-with? % "Configure.LeaveOuts.") old))
                                   "{ { Mind test Secondary } Codex [ CodexAgentsMd ] }")))

;; Ids: the three families the counters seed, read from any reply text.
(let [text (str "SendOutcomes.[ { send-outcome-000000000000056e logical-delivery-00000000000000014574 } ]"
                " Deliveries.[ { queue-attempt-0000000000000f08 logical-delivery-00000000000000014560 } ]")
      ids (flow-capsule/issued-ids text)]
  (check "every id of the three families is read from reply text"
         (= #{"send-outcome-000000000000056e" "logical-delivery-00000000000000014574"
              "queue-attempt-0000000000000f08" "logical-delivery-00000000000000014560"} ids))
  (check "a logical delivery reads in decimal, a queue attempt and a send outcome in hex"
         (= [[:logical-delivery 14574] [:queue-attempt 3848] [:send-outcome 1390]]
            (map flow-capsule/id-number ["logical-delivery-00000000000000014574"
                                         "queue-attempt-0000000000000f08"
                                         "send-outcome-000000000000056e"])))
  (check "the old maxima are each family's highest id"
         (= {:logical-delivery 14574 :queue-attempt 3848 :send-outcome 1390} (flow-capsule/old-maxima ids)))
  (check "a family with no id seen reads zero"
         (= {:logical-delivery 0 :queue-attempt 0 :send-outcome 0} (flow-capsule/old-maxima #{})))
  (check "the counters are each old maximum plus the margin, in decimal, in the ruled order"
         (= "Configure.Counters.{ 24574 13848 11390 }"
            (flow-capsule/counters-datom {:logical-delivery 14574 :queue-attempt 3848 :send-outcome 1390} 10000))))

(check "the counters are read from the end of a Configuration reply"
       (= [24574 13848 11390]
          (flow-capsule/configuration-counters
           "Configuration.{ { /src } [ ] [ ] 30 { 24574 13848 11390 } }")))
(check "a reply that is not a Configuration holds no counters"
       (nil? (flow-capsule/configuration-counters "exit 2: Connection refused (os error 111)")))

(let [old #{"logical-delivery-00000000000000000040" "queue-attempt-0000000000000004"}
      new #{"logical-delivery-00000000000000010041" "logical-delivery-00000000000000000040"
            "send-outcome-0000000000002711"}]
  (check "an id both Nexuses issued is reissued"
         (= ["logical-delivery-00000000000000000040"] (flow-capsule/reissued old new)))
  (check "an id below its seeded counter is listed, each family against its own"
         (= ["logical-delivery-00000000000000000040"]
            (flow-capsule/below-counters new [10040 10004 10001]))))

;; The guard: a Nexus owns the sockets unless the client found no socket or
;; was refused the connection; a reply it cannot read is still an owner.
(check "the guard sees an owner in an answer"
       (flow-capsule/owner-answers? "Listed.[]"))
(check "the guard sees an owner in a reply its client cannot read"
       (flow-capsule/owner-answers? "exit 1: received an invalid typed frame"))
(check "the guard sees no owner where there is no socket"
       (not (flow-capsule/owner-answers? "exit 2: No such file or directory (os error 2)")))
(check "the guard sees no owner where the connection is refused"
       (not (flow-capsule/owner-answers? "exit 2: Connection refused (os error 111)")))

;; The re-bind lines, made from the pane list as the procedure's C7 makes
;; them: one per labelled seat pane, the trackers' stand-ins first, then the
;; rest in pane order; a pane with no seat label is not typed to.
(let [panes [{:pane_id "w1:p1" :label "capsule-driver"}
             {:pane_id "w2:p1" :label "{ Psyche test Secondary 0a1b2c }"}
             {:pane_id "w3:p1" :label "{ Psyche test Quaternary 3d4e5f }"}
             {:pane_id "w4:p1" :label "{ Mind test Secondary 6a7b8c }"}
             {:pane_id "w5:p1" :label "{ Mind test Quaternary 9d0e1f }"}]
      lines (flow-capsule/rebind-lines panes "/nix/store/new" #(clojure.string/ends-with? % "Quaternary"))]
  (check "the trackers' stand-ins come first, then the rest in pane order"
         (= ["w3:p1" "w5:p1" "w2:p1" "w4:p1"] (map :pane lines)))
  (check "each line names its pane's FlowId and Address"
         (= {:pane "w2:p1" :flow-id "0a1b2c" :address "{ Psyche test Secondary }"}
            (dissoc (nth lines 2) :line)))
  (check "the typed line is the shell prefix and the new client's BindSeat"
         (= "! /nix/store/new/bin/flow 'BindSeat.{ Psyche test Secondary }'" (:line (nth lines 2)))))

;; The readback: each Awake seat's FlowId by Address; a seat whose FlowId
;; changed, went Asleep or is gone is a changed seat.
(let [before (flow-capsule/awake-seats
              (flow-capsule/read-datom
               "Listed.[ { { Psyche test Secondary } Awake.0a1b2c [] [] } { { Psyche test Quaternary } Awake.3d4e5f [] [] } { { Psyche test Primary } Asleep [] [] } ]"))
      after (flow-capsule/awake-seats
             (flow-capsule/read-datom
              "Listed.[ { { Psyche test Secondary } Awake.0a1b2c [] [] } { { Psyche test Quaternary } Awake.777777 [] [] } { { Psyche test Primary } Asleep [] [] } ]"))]
  (check "the Awake seats read by Address"
         (= {"{ Psyche test Secondary }" "0a1b2c" "{ Psyche test Quaternary }" "3d4e5f"} before))
  (check "a seat under another FlowId is changed"
         (= ["{ Psyche test Quaternary }"] (flow-capsule/changed-seats before after)))
  (check "a seat gone Asleep is changed"
         (= ["{ Psyche test Secondary }"] (flow-capsule/changed-seats {"{ Psyche test Secondary }" "0a1b2c"} {}))))

;; The window, from the hand stop: ready, end, each pane's type-to-Bound
;; time, and the extrapolation to the night's seat count plus the readback.
(check "the window is measured and extrapolated to the night's seats"
       (= {:ready-s 2.5 :end-s 30.0 :mean-pane-s 4.0 :readback-s 1.0 :extrapolated-s 85.0}
          (flow-capsule/window-measure {:start 1000 :ready 3500 :end 31000 :pane-ms [3000 5000]
                                        :readback-ms 1000 :seats 21})))

;; The seed, in the order the Nexus allows it: a full Nexus opens only a
;; store holding the Nexus declaration, and a configuration-only Nexus
;; refuses Counters NotRunning. So the Nexus is declared configuration-only
;; first; a full Nexus on the private socket then takes Counters, the lower
;; one and the Configuration read while no delivery row exists; the rest of
;; the replay follows configuration-only, in the file's order.
(let [replay ["Configure.Models.[ { Secondary Psyche haiku } ]"
              "Configure.Nexus.{ /src 60 }"
              "Configure.DefaultMinutes.30"
              "Configure.Metaflows.[ { { Psyche test Secondary } [ spirit:core ] [] Simplified Explicit.[] Maintained «» [] } ]"]
      plan (flow-capsule/seed-plan replay "Configure.Counters.{ 3 2 1 }" "Configure.Counters.{ 2 1 0 }")
      steps (map (juxt :via :datom) plan)]
  (check "the seed declares the Nexus first, through a configuration-only Nexus"
         (= [:configure-only "Configure.Nexus.{ /src 60 }"] (first steps)))
  (check "then a full Nexus takes Counters, the lower one and the Configuration read, before any other declaration"
         (= [[:running "Configure.Counters.{ 3 2 1 }"] [:running "Configure.Counters.{ 2 1 0 }"] [:running "Configuration"]]
            (take 3 (rest steps))))
  (check "the rest of the replay follows configuration-only, in the file's order"
         (= [[:configure-only "Configure.Models.[ { Secondary Psyche haiku } ]"]
             [:configure-only "Configure.DefaultMinutes.30"]
             [:configure-only "Configure.Metaflows.[ { { Psyche test Secondary } [ spirit:core ] [] Simplified Explicit.[] Maintained «» [] } ]"]]
            (drop 4 steps)))
  (check "no Counters goes to a configuration-only Nexus"
         (not-any? (fn [[via datom]] (and (= via :configure-only) (str/starts-with? datom "Configure.Counters."))) steps))
  (check "a replay without a Nexus declaration plans no seed"
         (nil? (flow-capsule/seed-plan (remove #(str/starts-with? % "Configure.Nexus.") replay)
                                       "Configure.Counters.{ 3 2 1 }" "Configure.Counters.{ 2 1 0 }"))))

;; The switch back, as the procedure means it: the declared start runs its
;; guard first and, while the old Nexus owns a socket, refuses without
;; starting the Nexus.
(let [started (atom 0)
      start! (fn [] (swap! started inc) :started)]
  (check "the declared start refuses while the old Nexus answers, and starts nothing"
         (and (= :refused (flow-capsule/declared-start {:ordinary "Listed.[]"
                                                        :meta "exit 2: No such file or directory (os error 2)"}
                                                       start!))
              (zero? @started)))
  (check "with no owner on either socket the declared start starts the Nexus"
         (= :started (flow-capsule/declared-start {:ordinary "exit 2: No such file or directory (os error 2)"
                                                   :meta "exit 2: Connection refused (os error 111)"}
                                                  start!))))

;; A queued request is witnessed by its own id: read from its SendAccepted,
;; none from a refusal, and no id is held by a text that lacks it.
(check "a queued Notice's id is read from its SendAccepted"
       (= "logical-delivery-00000000000000000009"
          (flow-capsule/accepted-id "SendAccepted.{ logical-delivery-00000000000000000009 9 Queued }")))
(check "a refused Send has no id"
       (nil? (flow-capsule/accepted-id "Refused.Unknown.Address.{ Psyche test Primary }")))
(check "a queue holds an id only where the id is written"
       (and (flow-capsule/holds-id? "Deliveries.[ { logical-delivery-00000000000000000009 } ]"
                                    "logical-delivery-00000000000000000009")
            (not (flow-capsule/holds-id? "Deliveries.[ ]" "logical-delivery-00000000000000000009"))))
(check "no id is held where there is no id"
       (not (or (flow-capsule/holds-id? "Deliveries.[ ]" nil) (flow-capsule/holds-id? "Deliveries.[ ]" ""))))

;; A seat's queue, read by id from a Listed reply.
(let [listed (flow-capsule/read-datom
              (str "Listed.[ { { Psyche test Quaternary } Asleep [] [ { logical-delivery-00000000000000000030 Sender.{ Psyche test Secondary } Queue Soft.Task.{ 1 «a» } } ] }"
                   " { { Psyche test Tertiary } Asleep [] [] } ]"))]
  (check "a seat's queued ids read from Listed"
         (= ["logical-delivery-00000000000000000030"] (flow-capsule/queued-ids listed "{ Psyche test Quaternary }")))
  (check "an empty queue reads no ids"
         (= [] (flow-capsule/queued-ids listed "{ Psyche test Tertiary }"))))

;; Scenario 20 needs a seat that takes its whole queue on a wake: a Regular
;; OnDemand seat, declared so in configure.datom (a Simplified seat takes
;; its oldest alone).
(let [metaflows (first (filter #(str/starts-with? % "Configure.Metaflows.") (flow-capsule/configure-lines)))]
  (check "scenario 20's seat is declared Regular OnDemand"
         (= ["Regular" "OnDemand"] (flow-capsule/seat-declaration metaflows flow-capsule/waking-seat)))
  (check "a Simplified seat reads as Simplified"
         (= ["Simplified" "OnDemand"] (flow-capsule/seat-declaration metaflows "{ Psyche test Quaternary }"))))

(if (seq @failures)
  (do (println (str (count @failures) " failed")) (System/exit 1))
  (println "all passed"))
