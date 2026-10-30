;; flow-capsule-test: checks the capsule runner's own logic that runs no
;; capsule: where a run keeps its witnesses, and what it copies there
;; before the state root is deleted.
;;
;; Usage: bb flow-capsule-test.clj <path to flow-capsule.clj>
(require '[babashka.fs :as fs]
         '[clojure.string :as str])

(load-file (first *command-line-args*))

(def failures (atom []))

(defn check [what ok]
  (println (str (if ok "pass" "FAIL") " | " what))
  (when-not ok (swap! failures conj what)))

;; Where witnesses go: FLOW_CAPSULE_WITNESS_DIR as given, else the calling
;; flow's directory's witnesses; nowhere is named when neither is set.
(check "FLOW_CAPSULE_WITNESS_DIR is taken as given"
       (= "/w/given" (flow-capsule/witness-root {"FLOW_CAPSULE_WITNESS_DIR" "/w/given"
                                                 "FLOW_DIRECTORY" "/f/622382"})))
(check "else the calling flow's directory names it"
       (= "/f/622382/witnesses" (flow-capsule/witness-root {"FLOW_DIRECTORY" "/f/622382"})))
(check "with neither, no witness root is named"
       (nil? (flow-capsule/witness-root {})))
(check "a run's directory is named by the flow version and a timestamp"
       (= "capsule-0.40.7-20261029T201500Z"
          (flow-capsule/witness-name "flow-nexus 0.40.7" (java.time.Instant/parse "2026-10-29T20:15:00.123Z"))))

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
