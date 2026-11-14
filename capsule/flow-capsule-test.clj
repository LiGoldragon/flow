;; flow-capsule-test: checks the capsule runner's own logic that runs no
;; capsule: where a run keeps its witnesses, and what it copies there
;; before the state root is deleted; and the takeover's readings: its
;; declarations, the ids and counters, the guard, the re-bind lines, the
;; seats read back and the window.
;;
;; Usage: bb flow-capsule-test.clj <path to flow-capsule.clj> <flow package>
(require '[babashka.fs :as fs] '[clojure.string :as str])

(load-file (first *command-line-args*))
(def flow-package (second *command-line-args*))

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
                "[ { logical-delivery-00000000000000000007 Sender.{ Psyche test Opus } Soft.Notice.«First of two. Tag w1.» }"
                " { logical-delivery-00000000000000000009 Flow Soft.Order.«Second of two. Tag w2.» } ]")]
  (check "a waking reads as one vector of its entries, oldest first"
         (= [{:id "logical-delivery-00000000000000000007" :sender "Sender Psyche test Opus" :priority "Soft"}
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
;; Codex Sol and an asleep OnDemand Fable are declared, and the old
;; Nexus is not given DefaultMinutes, which it does not know, and takes the
;; permaflows' leave-outs as a LeaveOuts family; the replay to the new Nexus
;; is the same declarations with DefaultMinutes and the leave-outs in place.
(let [lines ["Configure.Nexus.{ /src 60 }"
             "Configure.Models.[ { Opus haiku } { Terra luna } ]"
             "Configure.DefaultMinutes.30"
             (str "Configure.Metaflows.[ { { Psyche test Opus } None [ ClaudeMd ] [ spirit:core ] [] Simplified Explicit.[] Maintained «» [] }"
                  " { { Psyche test Sonnet } None [] [ spirit:core ] [] Regular Default OnDemand «» [] }"
                  " { { Psyche test Haiku } None [] [ spirit:core ] [] Simplified Explicit.[] OnDemand «» [] }"
                  " { { Mind test Terra } None [ CodexAgentsMd ] [ spirit:core ] [] Simplified Explicit.[] OnDemand «» [] } ]")]
      {:keys [old replay]} (flow-capsule/takeover-configuration lines)
      metaflows (first (filter #(clojure.string/starts-with? % "Configure.Metaflows.") old))]
  (check "the old Nexus is not given DefaultMinutes"
         (not-any? #(clojure.string/starts-with? % "Configure.DefaultMinutes.") old))
  (check "the replay carries every old declaration and DefaultMinutes, in the file's order"
         (= (map #(subs % 0 14) replay)
            ["Configure.Nexu" "Configure.Mode" "Configure.Defa" "Configure.Meta"]))
  (check "the old Nexus takes the leave-outs as a family, before the metaflows"
         (= (map #(subs % 0 14) old)
            ["Configure.Nexu" "Configure.Mode" "Configure.Leav" "Configure.Meta"]))
  (check "the replay's Codex Sol leaves out what the Codex Terra does, in place"
         (clojure.string/includes?
          (first (filter #(clojure.string/starts-with? % "Configure.Metaflows.") replay))
          "{ { Mind test Sol } None [ CodexAgentsMd ] [ spirit:core intent:models ] [] Simplified Explicit.[] Maintained «» [] }"))
  (check "the old declarations omit the new workspace position and the replay keeps it"
         (and (not (clojure.string/includes? metaflows "} None ["))
              (clojure.string/includes?
               (first (filter #(clojure.string/starts-with? % "Configure.Metaflows.") replay))
               "} None [")))
  (check "no kept seat is OnDemand but the asleep Fable"
         (= 1 (count (re-seq #"OnDemand" metaflows))))
  (check "the asleep Fable is declared OnDemand"
         (clojure.string/includes? metaflows "{ { Psyche test Fable } [ spirit:core intent:models ] [] Regular Default OnDemand «» [] }"))
  (check "a Codex Sol is declared Maintained"
         (clojure.string/includes? metaflows "{ { Mind test Sol } [ spirit:core intent:models ] [] Simplified Explicit.[] Maintained «» [] }"))
  (check "the Codex Sol runs the Codex Terra's model"
         (clojure.string/includes? (first (filter #(clojure.string/starts-with? % "Configure.Models.") old))
                                   "{ Sol luna }"))
  (check "the Codex Sol leaves out what the Codex Terra does"
         (clojure.string/includes? (first (filter #(clojure.string/starts-with? % "Configure.LeaveOuts.") old))
                                   "{ { Mind test Sol } Codex [ CodexAgentsMd ] }"))
  (check "the old LeaveOuts names each seat's leave-outs under its harness, and none for a seat without"
         (= (first (filter #(clojure.string/starts-with? % "Configure.LeaveOuts.") old))
            (str "Configure.LeaveOuts.[ { { Psyche test Opus } Claude [ ClaudeMd ] }"
                 " { { Mind test Terra } Codex [ CodexAgentsMd ] }"
                 " { { Mind test Sol } Codex [ CodexAgentsMd ] } ]"))))

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
             {:pane_id "w2:p1" :label "{ Psyche test Opus 0a1b2c }"}
             {:pane_id "w3:p1" :label "{ Psyche test Haiku 3d4e5f }"}
             {:pane_id "w4:p1" :label "{ Mind test Sol 6a7b8c }"}
             {:pane_id "w5:p1" :label "{ Mind test Terra 9d0e1f }"}]
      _ (reset! flow-capsule/package-interface {flow-package :place})
      lines (flow-capsule/rebind-lines panes flow-package #(or (clojure.string/ends-with? % "Haiku") (clojure.string/ends-with? % "Terra")))]
  (check "the trackers' stand-ins come first, then the rest in pane order"
         (= ["w3:p1" "w5:p1" "w2:p1" "w4:p1"] (map :pane lines)))
  (check "each line names its pane's FlowId and Address"
         (= {:pane "w2:p1" :flow-id "0a1b2c" :address "{ Psyche test Opus }"}
            (dissoc (nth lines 2) :line)))
  (check "the typed line gives the new client the query alone; the pane's configuration names main"
         (= (str "! " flow-package "/bin/flow-nexus 'BindSeat.{ Psyche test Opus }'")
            (:line (nth lines 2))))
  (check "a client of the new package takes the query alone"
         (= ["Metaflows"]
            (flow-capsule/client-arguments flow-package :client flow-capsule/seed-socket "Metaflows")))
  (check "main's configuration is the capsule's own, which the panes inherit"
         (= flow-capsule/config-home (flow-capsule/place-configuration-home flow-capsule/ordinary-socket)))
  (check "another place's configuration is a home of its own"
         (= (str flow-capsule/state-root "/config-next")
            (flow-capsule/place-configuration-home flow-capsule/seed-socket)))
  (let [runtime (str (fs/create-temp-dir))
        configuration (str (fs/create-temp-dir))
        client (flow-capsule/binary flow-package :client)
        env {"XDG_RUNTIME_DIR" runtime "XDG_CONFIG_HOME" configuration}
        missing (flow-capsule/run {:env env} client "Metaflows")
        _ (fs/create-dirs (str configuration "/nexus"))
        _ (spit (str configuration "/nexus/flow-nexus.datom") "{ no-owner }\n")
        configured (flow-capsule/run {:env env} client "Metaflows")
        typed (flow-capsule/run {:env env} client "no-owner" "Metaflows")]
    (check "the packaged client with no configuration is refused, naming the file"
           (and (= 2 (:exit missing)) (str/starts-with? (:err missing) "Missing.")
                (str/includes? (:err missing) (str configuration "/nexus/flow-nexus.datom"))))
    (check "the packaged client takes its place from its configuration, and reaches no Nexus there"
           (and (= 2 (:exit configured)) (not (str/includes? (:err configured) "invalid Flow Query"))
                (not (str/includes? (:err configured) "usage:"))
                (not (str/starts-with? (:err configured) "Missing."))))
    (check "a typed place is a second argument, refused"
           (and (= 2 (:exit typed)) (str/includes? (:err typed) "usage:"))))
  (check "a place-started Nexus is started with its place"
         (= (str "{ «main» «" (fs/path flow-capsule/places-root "main") "» «/state/capsule» }")
            (flow-capsule/start-text flow-package flow-capsule/ordinary-socket flow-capsule/meta-socket
                                     "/state/capsule/flow.sema"))))

;; The readback: each Awake seat's FlowId by Address; a seat whose FlowId
;; changed, went Asleep or is gone is a changed seat.
(let [before (flow-capsule/awake-seats
              (flow-capsule/read-datom
               "Listed.[ { { Psyche test Opus } Awake.0a1b2c [] [] } { { Psyche test Haiku } Awake.3d4e5f [] [] } { { Psyche test Fable } Asleep [] [] } ]"))
      after (flow-capsule/awake-seats
             (flow-capsule/read-datom
              "Listed.[ { { Psyche test Opus } Awake.0a1b2c [] [] } { { Psyche test Haiku } Awake.777777 [] [] } { { Psyche test Fable } Asleep [] [] } ]"))]
  (check "the Awake seats read by Address"
         (= {"{ Psyche test Opus }" "0a1b2c" "{ Psyche test Haiku }" "3d4e5f"} before))
  (check "a seat under another FlowId is changed"
         (= ["{ Psyche test Haiku }"] (flow-capsule/changed-seats before after)))
  (check "a seat gone Asleep is changed"
         (= ["{ Psyche test Opus }"] (flow-capsule/changed-seats {"{ Psyche test Opus }" "0a1b2c"} {}))))

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
(let [replay ["Configure.Models.[ { Opus haiku } ]"
              "Configure.Nexus.{ /src 60 }"
              "Configure.DefaultMinutes.30"
              "Configure.Metaflows.[ { { Psyche test Opus } None [] [ spirit:core ] [] Simplified Explicit.[] Maintained «» [] } ]"]
      plan (flow-capsule/seed-plan replay "Configure.Counters.{ 3 2 1 }" "Configure.Counters.{ 2 1 0 }")
      steps (map (juxt :via :datom) plan)]
  (check "the seed declares the Nexus first, through a configuration-only Nexus"
         (= [:configure-only "Configure.Nexus.{ /src 60 }"] (first steps)))
  (check "then a full Nexus takes Counters, the lower one and the Configuration read, before any other declaration"
         (= [[:running "Configure.Counters.{ 3 2 1 }"] [:running "Configure.Counters.{ 2 1 0 }"] [:running "Configuration"]]
            (take 3 (rest steps))))
  (check "the rest of the replay follows configuration-only, in the file's order"
         (= [[:configure-only "Configure.Models.[ { Opus haiku } ]"]
             [:configure-only "Configure.DefaultMinutes.30"]
             [:configure-only "Configure.Metaflows.[ { { Psyche test Opus } None [] [ spirit:core ] [] Simplified Explicit.[] Maintained «» [] } ]"]]
            (drop 4 steps)))
  (check "no Counters goes to a configuration-only Nexus"
         (not-any? (fn [[via datom]] (and (= via :configure-only) (str/starts-with? datom "Configure.Counters."))) steps))
  (let [place-steps (map (juxt :via :datom)
                          (flow-capsule/seed-plan replay "Configure.Counters.{ 3 2 1 }" "Configure.Counters.{ 2 1 0 }" :place))]
    (check "a Nexus that starts in its place takes every seed step running, in the same order"
           (and (every? #(= :running (first %)) place-steps)
                (= (map second steps) (map second place-steps)))))
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
       (nil? (flow-capsule/accepted-id "Refused.Unknown.Address.{ Psyche test Fable }")))
(check "a queue holds an id only where the id is written"
       (and (flow-capsule/holds-id? "Deliveries.[ { logical-delivery-00000000000000000009 } ]"
                                    "logical-delivery-00000000000000000009")
            (not (flow-capsule/holds-id? "Deliveries.[ ]" "logical-delivery-00000000000000000009"))))
(check "no id is held where there is no id"
       (not (or (flow-capsule/holds-id? "Deliveries.[ ]" nil) (flow-capsule/holds-id? "Deliveries.[ ]" ""))))

;; A seat's queue, read by id from a Listed reply.
(let [listed (flow-capsule/read-datom
              (str "Listed.[ { { Psyche test Haiku } Asleep [] [ { logical-delivery-00000000000000000030 Sender.{ Psyche test Opus } Queue Soft.Task.{ 1 «a» } } ] }"
                   " { { Psyche test Sonnet } Asleep [] [] } ]"))]
  (check "a seat's queued ids read from Listed"
         (= ["logical-delivery-00000000000000000030"] (flow-capsule/queued-ids listed "{ Psyche test Haiku }")))
  (check "an empty queue reads no ids"
         (= [] (flow-capsule/queued-ids listed "{ Psyche test Sonnet }"))))

;; The queued Notice lives in the Metaflows queue (Listed), not in the delivery
;; queue (ReadQueueDeliveries lists only attempts). It is witnessed there by
;; its id, before the stop and after each rollback.
(defn check-try [what f] (check what (try (boolean (f)) (catch Throwable _ false))))

(let [listed (str "Listed.[ { { Psyche test Fable } Asleep [ 5912d9 ] [ { logical-delivery-00000000000000000001 Sender.{ Psyche test Opus } Queue Soft.Notice.«Held» } ] }"
                  " { { Psyche test Sonnet } Awake.429d70 [] [] } ]")
      id "logical-delivery-00000000000000000001"
      primary "{ Psyche test Fable }"]
  (check-try "the Notice is found in the Metaflows queue of its Address"
             #(flow-capsule/notice-queued? listed primary id))
  (check-try "the Notice is not found at another Address"
             #(not (flow-capsule/notice-queued? listed "{ Psyche test Sonnet }" id)))
  (check-try "a blank or missing id is never found"
             #(not (or (flow-capsule/notice-queued? listed primary "") (flow-capsule/notice-queued? listed primary nil))))
  (check-try "an empty Metaflows holds no Notice"
             #(not (flow-capsule/notice-queued? "Listed.[]" primary id))))

;; A logical delivery's state is read from the sender's own ObserveSends first
;; frame (SendsObserved, one DeliverySnapshot per delivery), not from
;; ReadQueueDeliveries, which lists only unsettled attempts.
(let [observed (str "SendsObserved.{ 9 [ { 1 logical-delivery-00000000000000000001 Sender.{ Psyche test Opus } { Psyche test Fable } Queued }"
                    " { 8 logical-delivery-00000000000000000022 Sender.{ Psyche test Opus } { Psyche test Sonnet } Uncertain }"
                    " { 9 logical-delivery-00000000000000000023 Sender.{ Psyche test Opus } { Psyche test Sonnet } Delivered.«receipt» } ] }")]
  (check-try "each logical delivery's state is read from SendsObserved"
             #(= {"logical-delivery-00000000000000000001" "Queued"
                  "logical-delivery-00000000000000000022" "Uncertain"
                  "logical-delivery-00000000000000000023" "Delivered"}
                 (flow-capsule/delivery-states observed)))
  (check-try "a refusal or silence reads no states"
             #(and (= {} (flow-capsule/delivery-states "Refused.Unidentified.{ 1 2 }"))
                   (= {} (flow-capsule/delivery-states nil)))))

(let [before {"a" "Queued" "b" "Uncertain" "c" "Delivered"}]
  (check-try "unchanged states pass"
             #(flow-capsule/deliveries-preserved? before before))
  (check-try "an attempt settled at restart (Uncertain to Delivered) passes"
             #(flow-capsule/deliveries-preserved? before (assoc before "b" "Delivered")))
  (check-try "a delivery added after the stop does not matter"
             #(flow-capsule/deliveries-preserved? before (assoc before "d" "Queued")))
  (check-try "a lost delivery fails"
             #(not (flow-capsule/deliveries-preserved? before (dissoc before "a"))))
  (check-try "a delivery that went back (Delivered to Queued) fails"
             #(not (flow-capsule/deliveries-preserved? before (assoc before "c" "Queued"))))
  (check-try "a delivery that changed otherwise (Queued to Uncertain) fails"
             #(not (flow-capsule/deliveries-preserved? before (assoc before "a" "Uncertain"))))
  (check-try "no delivery read before the stop is no witness, so it fails"
             #(not (flow-capsule/deliveries-preserved? {} {"a" "Queued"}))))

;; Scenario 20 needs a seat that takes its whole queue on a wake: a Regular
;; OnDemand seat (a Simplified seat takes its oldest alone).
(let [metaflows (str "Configure.Metaflows.[ { { Psyche test Sonnet } None [ ClaudeMd ] [ spirit:core ] [] Regular Default OnDemand «» [] }"
                     " { { Psyche test Haiku } None [] [ spirit:core ] [] Simplified Explicit.[] OnDemand «» [] } ]")]
  (check "a seat's declaration reads its FlowKind and wake policy"
         (= ["Regular" "OnDemand"] (flow-capsule/seat-declaration metaflows "{ Psyche test Sonnet }")))
  (check "a Simplified seat reads as Simplified"
         (= ["Simplified" "OnDemand"] (flow-capsule/seat-declaration metaflows "{ Psyche test Haiku }"))))
;; and configure.datom declares scenario 20's seat so, where the file is
;; beside this one (the Nix check holds each file alone, so there it is not).
(let [datoms (fs/file (fs/parent (fs/canonicalize *file*)) "configure.datom")]
  (if (fs/exists? datoms)
    (check "scenario 20's seat is declared Regular OnDemand in configure.datom"
           (= ["Regular" "OnDemand"]
              (flow-capsule/seat-declaration (first (filter #(str/starts-with? % "Configure.Metaflows.")
                                                            (str/split-lines (slurp datoms))))
                                             flow-capsule/waking-seat)))
    (println "skip | scenario 20's seat in configure.datom: no configure.datom beside this file")))

;; Scenarios 21 and 22 read a resume from the harness's command line: Claude
;; started on the flow's own session with `--resume <session>`.
(check "a command line resuming the session reads as resumed"
       (flow-capsule/resumed-on? ["/bin/claude --model claude-haiku-5-5 --resume 1f2e-77 --name x"] "1f2e-77"))
(check "a fresh session, or another session resumed, does not"
       (and (not (flow-capsule/resumed-on? ["/bin/claude --session-id 1f2e-77"] "1f2e-77"))
            (not (flow-capsule/resumed-on? ["/bin/claude --resume 9a9a-00"] "1f2e-77"))
            (not (flow-capsule/resumed-on? ["/bin/claude --resume 1f2e-77"] nil))))
;; configure.datom declares the Haiku model's resumption, where the file is
;; beside this one.
(let [datoms (fs/file (fs/parent (fs/canonicalize *file*)) "configure.datom")]
  (if (fs/exists? datoms)
    (check "configure.datom declares claude-haiku-5-5's resumption"
           (some #(str/starts-with? % "Configure.Resumptions.[ { claude-haiku-5-5 ")
                 (str/split-lines (slurp datoms))))
    (println "skip | the resumption in configure.datom: no configure.datom beside this file")))

;; Every wait of the capsule answers its event: it arms its sources, asks its
;; question once armed, and asks again only when an event arrives; the
;; deadline bounds the wait for the next event and nothing else.
(require '[babashka.process :as p] '[clojure.java.io :as io] '[cheshire.core :as json])

(defn acting-first
  "A question that, asked first, sets off the action and answers nil, so
  only an event can bring its next asking; then the check."
  [action check]
  (let [first-ask (atom true)]
    #(if @first-ask (do (reset! first-ask false) (action) nil) (check))))

(defn queue [events]
  (let [q (java.util.concurrent.LinkedBlockingQueue.)]
    (doseq [event events] (.put q event))
    q))

;; A deadline far off: every answer here comes from an event.
(def later (+ (System/currentTimeMillis) 600000))

(let [asked (atom 0)
      answer (flow-capsule/answer-on later #(do (swap! asked inc) nil)
                                     (queue [:a :b :flow-capsule/ended]) 1)]
  (check "a question is asked once armed and again at each arrival, the events already queued taken together"
         (and (nil? answer) (= 2 @asked))))
(check "a question answered truthy ends the wait"
       (= :yes (flow-capsule/answer-on later (constantly :yes) (queue []) 1)))
(let [asked (atom 0)]
  (check "a wait ends when every source has ended"
         (nil? (flow-capsule/answer-on later #(do (swap! asked inc) nil)
                                       (queue [:flow-capsule/ended :flow-capsule/ended]) 2))))
(let [armed (atom 0)
      source (fn [events _] (swap! armed inc) (.put events :changed) (fn []))]
  (check "a question answering ::rearm arms its sources again"
         (and (= :settled (flow-capsule/await-events 30 (fn [] [source])
                                                     #(if (< @armed 2) :flow-capsule/rearm :settled)))
              (= 2 @armed))))
(let [closed (atom false)
      wait (flow-capsule/arm [(fn [events _] (.put events :seen) #(reset! closed true))])]
  (check "a source armed first keeps what it sees until the wait, and is closed after it"
         (and (= :seen-it (wait 30 (let [first-ask (atom true)]
                                     #(if @first-ask (do (reset! first-ask false) nil) :seen-it))))
              @closed)))

;; The metaflows: Observe.Metaflows opens on the metaflows listed, replaces or
;; adds one at each MetaflowChanged, drops one at each MetaflowGone; each
;; frame after the opening one is an event. A Nexus that opens on another
;; frame, or ends before opening, serves no subscription: its fallback is
;; armed instead (the store it writes).
(let [opening "MetaflowsObserved.[ { { Psyche test Fable } Asleep [] [] } { { Mind test Sol } Awake.c1a0de [] [] } ]"
      woke "MetaflowChanged.{ { Psyche test Fable } Awake.5ec0d1 [] [] }"
      fresh "MetaflowChanged.{ { Psyche test Haiku } Awake.f1c841 [] [] }"
      gone "MetaflowGone.{ Mind test Sol }"
      fold (fn [lines] (reduce flow-capsule/fold-metaflows nil (map flow-capsule/read-datom lines)))
      at (fn [lines address]
           (flow-capsule/metaflow (flow-capsule/listed-of (fold lines)) address))
      stream (fn [lines] (fn [] {:reader (java.io.BufferedReader. (java.io.StringReader. (str/join "\n" lines)))
                                 :close (fn [])}))
      no-fallback #(throw (ex-info "no fallback expected" {}))
      watched (fn [held source]
                (flow-capsule/await-events
                 600 [source]
                 #(when-let [listed (some-> @held flow-capsule/listed-of)]
                    (binding [flow-capsule/*listed* listed]
                      (:flow (flow-capsule/current "{ Psyche test Fable }"))))))]
  (check "the opening frame lists the metaflows"
         (= {:state :awake :flow "c1a0de" :past [] :queue []} (at [opening] "{ Mind test Sol }")))
  (check "a change replaces the metaflow at its Address"
         (= :awake (:state (at [opening woke] "{ Psyche test Fable }"))))
  (check "a change at a new Address adds it"
         (= "f1c841" (:flow (at [opening fresh] "{ Psyche test Haiku }"))))
  (check "a metaflow gone is no longer held"
         (and (nil? (at [opening gone] "{ Mind test Sol }"))
              (= 1 (count (fold [opening gone])))))
  (check "nothing is held before the opening frame"
         (nil? (fold [woke])))
  (let [held (atom nil)]
    (check "the question is asked at each frame, over the subscription's metaflows"
           (= "5ec0d1" (watched held (flow-capsule/frames-source held (stream [opening woke]) no-fallback)))))
  (let [held (atom nil)]
    (check "a stream that ends first answers the last answer"
           (nil? (watched held (flow-capsule/frames-source held (stream [opening]) no-fallback)))))
  (doseq [[what lines] [["opens on another frame" ["Refused.Store.«down»"]]
                        ["ends before opening" []]]]
    (let [held (atom :stale)
          fell-back (atom false)
          fallback (fn [] (fn [events _] (reset! fell-back true) (.put events :flow-capsule/ended) (fn [])))]
      (watched held (flow-capsule/frames-source held (stream lines) fallback))
      (check (str "a Nexus whose stream " what " serves no subscription: its fallback is armed")
             (and @fell-back (nil? @held))))))

;; Files and sockets: one inotifywait, armed (its watches established)
;; before the question is first asked.
(let [dir (str (fs/create-temp-dir {:prefix "capsule-events"}))
      place (str dir "/place")
      socket (str place "/flow.sock")
      wait (flow-capsule/arm [(flow-capsule/socket-events socket)])]
  (check "a socket's directory missing is made, mode 0700, before the watch"
         (= "rwx------" (fs/posix->str (fs/posix-file-permissions place))))
  (check "a socket moved to its name is seen at the move"
         (wait 30 (acting-first #(do (spit (str place "/.flow.sock.1") "")
                                     (fs/move (str place "/.flow.sock.1") socket))
                                #(fs/exists? socket))))
  (let [wait (flow-capsule/arm [(flow-capsule/socket-events socket)])
        mode #(fs/posix->str (fs/posix-file-permissions socket))]
    (check "a socket protected at its name is seen at the change"
           (wait 30 (acting-first #(fs/set-posix-file-permissions socket "rw-------")
                                  #(= "rw-------" (mode))))))
  (let [store (str dir "/flow.sema")
        wait (flow-capsule/arm [(flow-capsule/file-events store)])]
    (check "a file written is seen"
           (= "written\n" (wait 30 (acting-first #(spit store "written\n")
                                                 #(let [text (when (fs/exists? store) (slurp store))]
                                                    (when (= "written\n" text) text)))))))
  (let [sessions (str dir "/sessions")
        _ (fs/create-dirs sessions)
        wait (flow-capsule/arm [(flow-capsule/directory-events [sessions] (constantly true) flow-capsule/file-kinds
                                                               :recursive true)])
        rollout (str sessions "/2026/11/09/rollout.jsonl")]
    (check "a file written in a directory made under a recursive watch is seen"
           (wait 30 (acting-first #(do (fs/create-dirs (fs/parent rollout)) (spit rollout "Tag c1\n"))
                                  #(and (fs/exists? rollout) (str/includes? (slurp rollout) "Tag c1"))))))
  (check "a watch on a directory missing is refused"
         (try ((flow-capsule/directory-events [(str dir "/absent")] (constantly true) flow-capsule/file-kinds)
               (java.util.concurrent.LinkedBlockingQueue.) later)
              false
              (catch clojure.lang.ExceptionInfo _ true)))
  (fs/delete-tree dir))

;; Processes: a child's exit as its own waiter sees it; any process's exit by
;; its pidfd (util-linux waitpid). Each is ended here by closing its input.
(let [exited? (fn [pid] (let [stat (try (slurp (str "/proc/" pid "/stat")) (catch Exception _ nil))]
                          (or (nil? stat) (boolean (re-find #"\) Z " stat)))))]
  (let [proc (p/process ["cat"] {:out :string})
        wait (flow-capsule/arm [(flow-capsule/exit-of proc)])]
    (check "a child's exit is seen as it exits"
           (= :exited (wait 30 (acting-first #(.close (:in proc)) #(when-not (p/alive? proc) :exited))))))
  (let [proc (p/process ["cat"] {:out :string})
        pid (.pid (:proc proc))
        wait (flow-capsule/arm [(flow-capsule/exits-of-pids [pid])])]
    (check "a process's exit is seen through its pidfd"
           (= :exited (wait 30 (acting-first #(.close (:in proc)) #(when (exited? pid) :exited)))))))

;; Herdr: the capsule's session's socket, events.subscribe, armed at
;; subscription_started; each later line one event. A Herdr stood in for by
;; a socket of this test's own.
(let [dir (str (fs/create-temp-dir {:prefix "capsule-herdr"}))
      socket (str dir "/herdr.sock")
      address (java.net.UnixDomainSocketAddress/of socket)
      server (doto (java.nio.channels.ServerSocketChannel/open java.net.StandardProtocolFamily/UNIX) (.bind address))
      asked (promise)
      release (promise)
      herdr (future
              (let [peer (.accept server)
                    reader (io/reader (java.nio.channels.Channels/newInputStream peer))
                    out (java.nio.channels.Channels/newOutputStream peer)
                    say #(do (.write out (.getBytes (str % "\n") "UTF-8")) (.flush out))]
                (deliver asked (.readLine ^java.io.BufferedReader reader))
                (say "{\"id\":\"flow-capsule\",\"result\":{\"type\":\"subscription_started\"}}")
                (say "{\"event\":\"pane.output_matched\",\"data\":{\"pane_id\":\"w1:p2\",\"matched_line\":\"Tag ra2\"}}")
                @release
                (.close peer)))
      seen (atom [])
      answer (with-redefs [flow-capsule/herdr-socket socket]
               (flow-capsule/await-events 30 [(flow-capsule/herdr-events [(flow-capsule/output-match "w1:p2" "Tag ra2")] seen)]
                                          #(seq @seen)))]
  (deliver release true)
  @herdr
  (.close server)
  (check "Herdr is asked for the subscriptions named"
         (= {:method "events.subscribe"
             :params {:subscriptions [{:type "pane.output_matched" :pane_id "w1:p2" :source "recent" :lines 400
                                       :match {:type "substring" :value "Tag ra2"}}]}}
            (select-keys (json/parse-string (deref asked 1000 "{}") true) [:method :params])))
  (check "an event Herdr sends after subscription_started is seen"
         (= "Tag ra2" (get-in (first answer) [:data :matched_line])))
  (fs/delete-tree dir))

(if (seq @failures)
  (do (println (str (count @failures) " failed")) (System/exit 1))
  (println "all passed"))
