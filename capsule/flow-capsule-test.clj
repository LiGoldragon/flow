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

;; A typed invocation failure keeps the exact controlled query and composer
;; refusal, while the scenario renderer emits only the four diagnostic fields.
(let [saved @flow-capsule/package-interface]
  (try
    (reset! flow-capsule/package-interface {flow-package :invocation})
    (let [failure (try
                    (with-redefs [flow-capsule/run (fn [& _] {:exit 2 :out "" :err "typed refusal"})]
                      (flow-capsule/client-argument flow-package "flow-meta" "/private/meta.sock" "Inspect.{ Mind test Terra }")
                      nil)
                    (catch Exception e (ex-data e)))]
      (check "a typed composer refusal keeps its client, query, and exact stderr"
             (= {:package flow-package :client "flow-meta" :query "Inspect.{ Mind test Terra }" :error "typed refusal"}
                failure)))
    (finally (reset! flow-capsule/package-interface saved))))

(let [saved @flow-capsule/results]
  (try
    (reset! flow-capsule/results [])
    (with-out-str
      (flow-capsule/scenario "typed diagnostic"
                             #(throw (ex-info "typed client invocation composition failed"
                                              {:package flow-package :client "flow" :query "Metaflows"
                                               :error "typed refusal" :credential "must-not-print"}))))
    (let [reply (:reply (last @flow-capsule/results))]
      (check "a scenario prints the controlled typed diagnostic"
             (and (str/includes? reply ":query \"Metaflows\"")
                  (str/includes? reply ":error \"typed refusal\"")))
      (check "a scenario omits unrelated exception data"
             (not (str/includes? reply "must-not-print"))))
    (finally (reset! flow-capsule/results saved))))

;; H0 establishes every identifier the later handover scenarios use. A missing
;; seat, flow or native thread ends the dependent scenario with H0's exact
;; launch refusals; it is never composed into a client query or observer argv.
(let [saved @flow-capsule/handover-h0-refusals
      refusals {:luna "StartRejected.Luna" :terra "StartRejected.Terra"}]
  (try
    (reset! flow-capsule/handover-h0-refusals refusals)
    (let [queries (atom [])]
      (with-redefs [flow-capsule/flow-meta
                    (fn [query] (swap! queries conj query) "Events.[]")]
        (check "a valid flow id composes the exact ReadEvents query"
               (and (= 0 (flow-capsule/stops-of "abcdef"))
                    (= ["ReadEvents.abcdef"] @queries)))))
    (let [called (atom false)
          failure (try
                    (with-redefs [flow-capsule/flow-meta (fn [_] (reset! called true) "Events.[]")]
                      (flow-capsule/stops-of ""))
                    nil
                    (catch Exception e (ex-data e)))]
      (check "a missing flow id fails before ReadEvents composition"
             (and (not @called)
                  (= "H0" (:prerequisite failure))
                  (= :flow (:identifier failure))
                  (= refusals (:h0-refusals failure)))))
    (let [called (atom false)
          failure (try
                    (with-redefs [flow-capsule/observer-json
                                  (fn [& _] (reset! called true) {})]
                      (flow-capsule/observer-history "missing-thread" nil))
                    nil
                    (catch Exception e (ex-data e)))]
      (check "a missing native thread fails before an observer query"
             (and (not @called)
                  (= :native-thread (:identifier failure))
                  (= refusals (:h0-refusals failure)))))
    (let [failure (try
                    (flow-capsule/require-handover-identifier
                     :seat-pane nil "{ Mind test Terra }")
                    nil
                    (catch Exception e (ex-data e)))]
      (check "a missing seat binding keeps its address and H0 refusals"
             (and (= :seat-pane (:identifier failure))
                  (= "{ Mind test Terra }" (:address failure))
                  (= refusals (:h0-refusals failure)))))
    (finally (reset! flow-capsule/handover-h0-refusals saved))))

;; Codex completion is proved from the exact known thread and complete prompt,
;; never from a Flow Stop count or a tag substring.
(let [prompt "Answer exactly. Tag same."
      turn (fn turn
             ([id text status] (turn id text status ::missing))
             ([id text status error]
              (cond-> {:id id :status status
                       :items [{:type "userMessage"
                                :content [{:type "text" :text text :text_elements []}]}]}
                (not= error ::missing) (assoc :error error))))
      history (fn [thread turns] {:thread {:id thread} :turns turns})
      proof #(flow-capsule/completed-turn-proof "thread-a" prompt #{"old"} %)
      valid (history "thread-a" [(turn "old" "old prompt" "completed" nil)
                                  (turn "new" prompt "completed" nil)])]
  (check "one fresh exact-prompt turn completed with explicit null error is established"
         (:established (proof valid)))
  (check "the pinned optional error field may be missing and still means no error"
         (:established (proof (history "thread-a" [(turn "old" "old prompt" "completed")
                                                    (turn "new" prompt "completed")]))))
  (check "a stale exact turn is not fresh"
         (not (:established (flow-capsule/completed-turn-proof
                             "thread-a" prompt #{"old" "new"} valid))))
  (check "a history returned for the wrong native thread is refused"
         (not (:established (proof (history "thread-b" [(turn "old" "old" "completed")
                                                         (turn "new" prompt "completed")])))))
  (check "the same tag in a different full prompt is refused"
         (not (:established (proof (history "thread-a" [(turn "old" "old" "completed")
                                                         (turn "new" (str "prefix " prompt) "completed")])))))
  (check "two fresh exact matching turns are ambiguous"
         (not (:established (proof (history "thread-a" [(turn "old" "old" "completed")
                                                         (turn "new-1" prompt "completed")
                                                         (turn "new-2" prompt "completed")])))))
  (check "completion on a different fresh turn does not complete the prompt turn"
         (not (:established (proof (history "thread-a" [(turn "old" "old" "completed")
                                                         (turn "prompt" prompt "inProgress" nil)
                                                         (turn "other" "other" "completed" nil)])))))
  (check "a non-null error refuses a completed turn"
         (not (:established (proof (history "thread-a" [(turn "old" "old" "completed")
                                                         (turn "new" prompt "completed" {:message "failed"})])))))
  (check "an in-progress exact turn is not completion"
         (not (:established (proof (history "thread-a" [(turn "old" "old" "completed")
                                                         (turn "new" prompt "inProgress" nil)])))))
  (check "an already-completed initial exact turn proves launch identity"
         (:established (flow-capsule/initial-turn-proof
                        "thread-a" prompt (history "thread-a" [(turn "initial" prompt "completed" nil)]))))
  (check "an initial turn on the wrong thread is refused"
         (not (:established (flow-capsule/initial-turn-proof
                             "thread-a" prompt (history "thread-b" [(turn "initial" prompt "completed" nil)])))))
  (doseq [[label malformed]
          [["a completed turn with a missing identity is refused"
            (history "thread-a" [(turn "old" "old" "completed")
                                  (dissoc (turn "new" prompt "completed") :id)])]
           ["a blank native thread identity is refused"
            (history "" [(turn "new" prompt "completed")])]
           ["duplicate turn identities are refused"
            (history "thread-a" [(turn "old" "old" "completed")
                                  (turn "old" prompt "completed")])]
           ["an absent turns vector is refused" {:thread {:id "thread-a"}}]
           ["a malformed turn items shape is refused"
            (history "thread-a" [(assoc (turn "new" prompt "completed") :items nil)])]]]
    (check label (not (:established (proof malformed)))))
  (check "an initial completed turn with no identity is refused"
         (not (:established
               (flow-capsule/initial-turn-proof
                "thread-a" prompt
                (history "thread-a" [(dissoc (turn "initial" prompt "completed") :id)])))))
  (doseq [[label malformed]
          [["wrong-thread baseline" (history "thread-b" [(turn "old" "old" "completed")])]
           ["absent-turn baseline" {:thread {:id "thread-a"}}]
           ["duplicate-id baseline" (history "thread-a" [(turn "old" "a" "completed")
                                                            (turn "old" "b" "completed")])]]]
    (let [effects (atom 0)
          failure (try
                    (let [_before (flow-capsule/observer-baseline! label "thread-a" malformed)]
                      (swap! effects inc))
                    nil
                    (catch Exception e (ex-data e)))]
      (check (str label " refuses before a native side effect")
             (and (zero? @effects) (= :observer-baseline (:identifier failure)))))))

(let [saved @flow-capsule/handover-fixture-outcomes]
  (try
    (reset! flow-capsule/handover-fixture-outcomes
            {:touch {:pass false :reply "original touch failure"}
             :t2 {:pass false :reply "original t2 failure"}
             :pred {:pass false :reply "original pred failure"}})
    (doseq [failed [:touch :t2 :pred]]
      (let [effects (atom 0)
            started (System/nanoTime)
            outcome ((flow-capsule/fixture-case :dependent [failed]
                                                #(do (swap! effects inc) {:pass true})))
            elapsed-ms (/ (- (System/nanoTime) started) 1000000.0)]
        (check (str "missing " (name failed) " is immediately NOT ESTABLISHED with no side effect")
               (and (not (:pass outcome)) (:not-established outcome)
                    (= failed (:failed-prerequisite outcome))
                    (= "original " (subs (get-in outcome [:prerequisite-outcome :reply]) 0 9))
                    (zero? @effects) (< elapsed-ms 100.0)))))
    (let [effects (atom 0)
          outcome ((flow-capsule/fixture-case :independent []
                                              #(do (swap! effects inc) {:pass true})))]
      (check "an independent fixture still runs after unrelated failures"
             (and (:pass outcome) (= 1 @effects))))
    (check "a NOT ESTABLISHED outcome is never accepted as pass or skip"
           (let [outcome ((flow-capsule/fixture-case :dependent [:touch] #(identity {:pass true})))]
             (and (false? (:pass outcome)) (not (:skipped outcome)))))
    (finally (reset! flow-capsule/handover-fixture-outcomes saved))))

(doseq [[label proof accepted]
        [["two exact typed refresh effects and both completions"
          (flow-capsule/refresh-effects-proof "Refreshed.two" "two" {:turn-id "a"}
                                              "Refreshed.three" "three" {:turn-id "b"} "one") true]
         ["typed pane reply mismatch"
          (flow-capsule/refresh-effects-proof "" "two" {:turn-id "a"}
                                              "Refreshed.three" "three" {:turn-id "b"} "one") false]
         ["successor completion missing"
          (flow-capsule/refresh-effects-proof "Refreshed.two" "two" nil
                                              "Refreshed.three" "three" {:turn-id "b"} "one") false]
         ["successor identities reused"
          (flow-capsule/refresh-effects-proof "Refreshed.two" "two" {:turn-id "a"}
                                              "Refreshed.two" "two" {:turn-id "b"} "one") false]]]
  (check label (= accepted (:established proof))))

(doseq [missing [:touch :t2 :pred]]
  (let [started (System/nanoTime)
        failure (try
                  (flow-capsule/require-fixture-values! missing {missing nil})
                  nil
                  (catch Exception e (ex-data e)))]
    (check (str "missing required " (name missing) " refuses before a wait")
           (and (= missing (:prerequisite failure))
                (= [missing] (:missing failure))
                (< (/ (- (System/nanoTime) started) 1000000.0) 100.0)))))

(let [base {:flow-kind "Regular" :user-modules [] :raw-records []
            :predecessor-depth 0 :purpose "" :prelaunch-observed true
            :profile-brief flow-capsule/brief :herdr-session flow-capsule/session
            :queued-first nil}]
  (check "the exact handover profile premises establish the source-derived first prompt"
         (:established (flow-capsule/handover-profile-proof base)))
  (doseq [[label drift]
          [["flow kind drift" {:flow-kind "Simplified"}]
           ["user module drift" {:user-modules ["vision:extra"]}]
           ["raw remembered record drift" {:raw-records ["raw"]}]
           ["unobserved prelaunch state" {:prelaunch-observed false}]
           ["purpose drift" {:purpose "purpose"}]
           ["brief drift" {:profile-brief (str flow-capsule/brief "\n")}]
           ["session drift" {:herdr-session "other"}]
           ["queued first-prompt content" {:queued-first "Soft.Order"}]]]
    (check label (not (:established (flow-capsule/handover-profile-proof (merge base drift)))))))

(let [line (first (filter #(str/starts-with? % "Configure.Metaflows.")
                          (str/split-lines
                           (slurp (fs/file (fs/parent (first *command-line-args*))
                                          "configure-handover.datom")))))
      maintained (str/replace-first line "Regular Default OnDemand"
                                    "Regular Default Maintained")
      profile (flow-capsule/seat-profile-declaration maintained flow-capsule/ho-seat)
      nested-profile (flow-capsule/seat-profile-declaration
                      (str "Configuration.{ Success [ " maintained " ] }")
                      flow-capsule/ho-seat)
      request (flow-capsule/launch-profile-request
               (flow-capsule/launch-query "Launch" flow-capsule/ho-seat)
               flow-capsule/ho-seat)]
  (check "the actual handover declaration parses as Regular with no user modules, raw records or purpose"
         (and (= "Regular" (:flow-kind profile))
              (= "Maintained" (:wake profile))
              (empty? (:user-modules profile))
              (empty? (:raw-records profile))
              (= "" (:purpose profile))))
  (check "the profile is found inside a typed Configuration snapshot"
         (= profile nested-profile))
  (check "the typed Launch request supplies exact Predecessors and brief"
         (= {:predecessor-depth 0 :profile-brief flow-capsule/brief} request)))

(let [reply (str "Inspected.{ { { Mind test Terra } Awake.flow-a [] [] } "
                 "{ None Some.{ native thread-a } } [] "
                 "[ Available.{ one two pane-a three } ] None }")
      binding (flow-capsule/inspected-binding reply flow-capsule/ho-seat)]
  (check "one Inspect reply binds its own Flow, native thread and pane"
         (= {:flow "flow-a" :thread "thread-a" :pane "pane-a" :past [] :queue []}
            binding))
  (check "an Inspect reply for a different Flow address is refused"
         (try (flow-capsule/inspected-binding reply flow-capsule/ho-other)
              false
              (catch Exception _ true))))

(let [stable (flow-capsule/binding-proof "flow-a" "thread-a"
                                         {:flow "flow-a" :thread "thread-a"}
                                         {:flow "flow-a" :thread "thread-a"})]
  (check "the expected successor Flow/native binding is stable across observation"
         (:established stable))
  (check "a successor Flow binding race is refused"
         (not (:established (flow-capsule/binding-proof
                             "flow-a" "thread-a"
                             {:flow "flow-a" :thread "thread-a"}
                             {:flow "flow-b" :thread "thread-a"}))))
  (check "a successor native-thread binding race is refused"
         (not (:established (flow-capsule/binding-proof
                             "flow-a" "thread-a"
                             {:flow "flow-a" :thread "thread-a"}
                             {:flow "flow-a" :thread "thread-b"})))))

(let [commit (apply str (repeat 40 "a"))
      blob (apply str (repeat 40 "b"))
      key {:address flow-capsule/ho-seat :generation 7
           :predecessor "pred" :successor "succ"}
      identity {:commit commit :path "Mind/test/Terra/pred.md" :blob blob}
      snapshot {:keys [key]}
      later [{:kind :archive :key key :identity identity}
             {:kind :removed :key key}]
      git {:commit-exit 0 :verified-commit commit :blob-exit 0 :blob blob
           :content-exit 0 :content "document" :requested-content "document"}
      verdict #(flow-capsule/retirement-verdict
                %1 %2 "pred" "succ" "Mind/test/Terra/pred.md" %3)]
  (check "exact retirement key, archive identity, order and removal complete"
         (:complete (verdict snapshot later git)))
  (check "a wrong retirement successor is refused"
         (not (:complete (verdict {:keys [(assoc key :successor "wrong")]} later git))))
  (check "a removal for a different generation key is refused"
         (not (:complete (verdict snapshot
                                  [(first later) {:kind :removed :key (assoc key :generation 8)}]
                                  git))))
  (check "removal before archive is refused"
         (not (:complete (verdict snapshot (vec (reverse later)) git))))
  (check "a wrong immutable commit:path/blob identity is refused"
         (not (:complete (verdict snapshot
                                  [{:kind :archive :key key
                                    :identity (assoc identity :path "wrong.md")}
                                   {:kind :removed :key key}]
                                  git)))))

(let [commit (apply str (repeat 40 "c"))
      blob (apply str (repeat 40 "d"))
      key {:address flow-capsule/ho-seat :generation 9
           :predecessor "flow-one" :successor "flow-two"}
      path "Mind/test/Terra/flow-one.md"
      content (str "generation=9 predecessor=flow-one successor=flow-two "
                   "sections=[Section] touch=None\n")
      frames [{:kind :snapshot :address flow-capsule/ho-seat :keys []}
              {:kind :closed :key key}
              {:kind :archive :key key
               :identity {:commit commit :path path :blob blob}}
              {:kind :removed :key key}]
      git {:commit-exit 0 :verified-commit commit :blob-exit 0 :blob blob
           :content-exit 0 :content content}
      proof #(flow-capsule/archived-untouched-generation-proof
              %1 "flow-one" "flow-two" path %2)]
  (check "an exact immutable closed generation explicitly records touch None"
         (:established (proof frames git)))
  (doseq [[label changed-frames changed-git]
          [["touch omission is refused"
            frames (assoc git :content (str/replace content " touch=None" ""))]
           ["a touched generation is refused"
            frames (assoc git :content (str/replace content "None" "Some(Unknown)"))]
           ["the wrong archived generation is refused"
            frames (assoc git :content (str/replace content "generation=9" "generation=8"))]
           ["the wrong successor is refused"
            frames (assoc git :content (str/replace content "successor=flow-two" "successor=other"))]
           ["an uncommitted archive identity is refused" frames (assoc git :commit-exit 1)]
           ["a wrong archive path is refused"
            (assoc-in frames [2 :identity :path] "Mind/test/Terra/other.md") git]
           ["a missing generation removal is refused" (pop frames) git]
           ["an inherited closed key is refused" (assoc-in frames [0 :keys] [key]) git]]]
    (check label (not (:established (proof changed-frames changed-git))))))

;; The H0-only diagnostic keeps its bounded evidence before teardown, even
;; when the launch body or an evidence probe fails. Credential/configuration
;; files are outside the preservation allowlist.
(let [scratch (str (fs/create-temp-dir))
      private (str scratch "/private")
      codex (str private "/codex")
      witness (str scratch "/witness")
      log (str private "/nexus.log")
      saved-witness @flow-capsule/witness-dir
      saved-log @flow-capsule/run-log]
  (try
    (doseq [dir [(str codex "/sessions/2026/11") (str codex "/archived_sessions")]]
      (fs/create-dirs dir))
    (spit (str codex "/sessions/2026/11/live.jsonl") "session\n")
    (spit (str codex "/archived_sessions/archived.jsonl") "archived\n")
    (spit (str codex "/session_index.jsonl") "index\n")
    (spit (str codex "/auth.json") "credential-must-not-copy\n")
    (spit (str codex "/config.toml") "configuration-must-not-copy\n")
    (spit log "native server log\n")
    (reset! flow-capsule/witness-dir witness)
    (reset! flow-capsule/run-log log)
    (with-redefs [flow-capsule/codex-home codex
                  flow-capsule/h0-herdr-stdout (str private "/absent-herdr-stdout.log")
                  flow-capsule/h0-herdr-stderr (str private "/absent-herdr-stderr.log")]
      (flow-capsule/preserve-h0-diagnostic!))
    (fs/delete-tree private)
    (check "H0 preservation keeps live and archived native JSONL before private teardown"
           (and (fs/exists? (str witness "/codex-sessions/2026/11/live.jsonl"))
                (fs/exists? (str witness "/codex-archived-sessions/archived.jsonl"))
                (fs/exists? (str witness "/codex-session-index/session_index.jsonl"))
                (fs/exists? (str witness "/nexus-and-native.log"))))
    (check "H0 preservation excludes login and configuration trees"
           (and (not (fs/exists? (str witness "/auth.json")))
                (not (fs/exists? (str witness "/config.toml")))
                (not (str/includes? (slurp (str witness "/handover-H0-preservation.json"))
                                    "credential-must-not-copy"))))
    (finally
      (reset! flow-capsule/witness-dir saved-witness)
      (reset! flow-capsule/run-log saved-log)
      (fs/delete-tree scratch))))

;; Full handover preservation is the one owner of its Codex evidence. The
;; later generic teardown copier keeps its other witnesses without colliding
;; with live or archived JSONL and their session index.
(let [scratch (str (fs/create-temp-dir))
      private (str scratch "/private")
      codex (str private "/codex")
      witness (str scratch "/witness")
      log (str private "/nexus.log")
      saved-witness @flow-capsule/witness-dir
      saved-log @flow-capsule/run-log]
  (try
    (doseq [dir [(str codex "/sessions/2026/11") (str codex "/archived_sessions")]]
      (fs/create-dirs dir))
    (spit (str codex "/sessions/2026/11/live.jsonl") "live\n")
    (spit (str codex "/archived_sessions/archived.jsonl") "archived\n")
    (spit (str codex "/session_index.jsonl") "index\n")
    (spit log "native log\n")
    (reset! flow-capsule/witness-dir witness)
    (reset! flow-capsule/run-log log)
    (with-redefs [flow-capsule/codex-home codex
                  flow-capsule/h0-herdr-stdout (str private "/absent-herdr-stdout.log")
                  flow-capsule/h0-herdr-stderr (str private "/absent-herdr-stderr.log")]
      (flow-capsule/preserve-full-handover!))
    (flow-capsule/keep-witnesses!
     witness (flow-capsule/teardown-witnesses true log
                                                (str private "/absent-claude")
                                                (str codex "/sessions")))
    (fs/delete-tree private)
    (check "full handover preservation keeps live, archived, and index evidence through teardown"
           (and (fs/exists? (str witness "/codex-sessions/2026/11/live.jsonl"))
                (fs/exists? (str witness "/codex-archived-sessions/archived.jsonl"))
                (fs/exists? (str witness "/codex-session-index/session_index.jsonl"))
                (fs/exists? (str witness "/nexus.log"))
                (not (fs/exists? private))))
    (check "ordinary handover teardown has no second Codex-session copier"
           (not (contains? (flow-capsule/teardown-witnesses true log "claude" "codex")
                           "codex-sessions")))
    (check "non-handover teardown retains its Codex-session copier"
           (= "codex" (get (flow-capsule/teardown-witnesses false log "claude" "codex")
                            "codex-sessions")))
    (finally
      (reset! flow-capsule/witness-dir saved-witness)
      (reset! flow-capsule/run-log saved-log)
      (fs/delete-tree scratch))))

(doseq [[label body throws?]
        [["success" #(identity :done) false]
         ["exception" #(throw (ex-info "handover failure" {})) true]]]
  (let [events (atom [])
        threw (try
                (flow-capsule/run-with-teardown!
                 #(do (swap! events conj :body) (body))
                 #(swap! events conj :sampler-quiesced)
                 #(do (swap! events conj :teardown) true))
                false
                (catch Exception _ true))]
    (check (str "handover quiesces its sampler and tears down exactly once after " label)
           (and (= throws? threw) (= [:body :sampler-quiesced :teardown] @events)))))

(let [kept (with-redefs [flow-capsule/handover-mode true
                         flow-capsule/h0-diagnostic-mode false
                         flow-capsule/preserve-full-handover!
                         #(throw (ex-info "preservation failed" {}))]
             (flow-capsule/preserve-teardown-witnesses!))]
  (check "a full-handover preservation error stays scoped so teardown can continue"
         (and (str/includes? kept "preservation failed")
              (not @flow-capsule/handover-evidence-preserved))))

(let [scratch (str (fs/create-temp-dir))
      saved @flow-capsule/witness-dir]
  (try
    (spit (str scratch "/handover-full-preservation.json") "destination already exists\n")
    (reset! flow-capsule/witness-dir scratch)
    (let [kept (with-redefs [flow-capsule/handover-mode true
                             flow-capsule/h0-diagnostic-mode false
                             flow-capsule/preserve-full-handover!
                             (fn [] {:files [{:present true :destination "existing"}
                                             {:present false :error "partial copy"}]
                                     :persistence {:persisted true}})]
                 (flow-capsule/preserve-teardown-witnesses!))]
      (check "a destination plus a partial preservation error is capture-incomplete"
             (and (str/includes? kept "incomplete")
                  (not @flow-capsule/handover-evidence-preserved))))
    (finally (reset! flow-capsule/witness-dir saved) (fs/delete-tree scratch))))

(let [scratch (str (fs/create-temp-dir))
      witness (str scratch "/witness")
      codex (str scratch "/codex")
      log (str scratch "/nexus.log")
      saved-witness @flow-capsule/witness-dir
      saved-log @flow-capsule/run-log]
  (try
    (doseq [dir [(str codex "/sessions") (str codex "/archived_sessions") witness]]
      (fs/create-dirs dir))
    (spit (str codex "/sessions/live.jsonl") "live\n")
    (spit (str codex "/archived_sessions/archived.jsonl") "archived\n")
    (spit (str codex "/session_index.jsonl") "index\n")
    (spit log "native log\n")
    ;; This receipt is deliberately stale. A failure to persist the receipt
    ;; for this invocation must not accept it as current evidence.
    (spit (str witness "/handover-full-preservation.json") "stale\n")
    (reset! flow-capsule/witness-dir witness)
    (reset! flow-capsule/run-log log)
    (let [kept (with-redefs [flow-capsule/handover-mode true
                             flow-capsule/h0-diagnostic-mode false
                             flow-capsule/codex-home codex
                             flow-capsule/h0-herdr-stdout (str scratch "/absent.stdout")
                             flow-capsule/h0-herdr-stderr (str scratch "/absent.stderr")
                             flow-capsule/persist-preservation-receipt!
                             (fn [& _] (throw (ex-info "current receipt persistence failed" {})))]
                 (flow-capsule/preserve-teardown-witnesses!))]
      (check "a stale receipt cannot mask current persistence failure after real leaf copies"
             (and (fs/exists? (str witness "/codex-sessions/live.jsonl"))
                  (fs/exists? (str witness "/codex-archived-sessions/archived.jsonl"))
                  (= "stale\n" (slurp (str witness "/handover-full-preservation.json")))
                  (str/includes? kept "incomplete")
                  (not @flow-capsule/handover-evidence-preserved))))
    (finally
      (reset! flow-capsule/witness-dir saved-witness)
      (reset! flow-capsule/run-log saved-log)
      (fs/delete-tree scratch))))

(let [events (atom [])
      threw (with-redefs [flow-capsule/stop-h0-sampler!
                          #(do (swap! events conj :sampler-stop)
                               (throw (ex-info "sampler stop failed" {})))
                          flow-capsule/append-h0-json!
                          (fn [& _] (swap! events conj :stop-failure-evidence))
                          flow-capsule/record-handover-finish-error!
                          (fn [_]
                            (swap! events conj :finish-report-failed)
                            (throw (ex-info "finish report failed" {})))]
              (try
                (flow-capsule/run-with-teardown!
                 #(swap! events conj :body)
                 flow-capsule/finish-h0-diagnostic!
                 #(do (swap! events conj :teardown) true))
                false
                (catch Exception _ true)))]
  (check "a real sampler-stop and finish-report failure still tears down exactly once"
         (and threw
              (= [:body :sampler-stop :stop-failure-evidence :finish-report-failed :teardown]
                 @events)
              (not @flow-capsule/handover-finish-succeeded))))

(let [calls (atom 0)
      clean (flow-capsule/run-with-teardown! #() #() #(do (swap! calls inc) false))]
  (check "a delete failure is returned after exactly one teardown"
         (and (false? clean) (= 1 @calls))))

(let [header (str "#!/bin/sh\nexport CODEX_HOME=" flow-capsule/codex-home "\n")
      exec (str "exec " flow-capsule/codex-binary " \"$@\"\n")
      original (str header exec)
      diagnostic (flow-capsule/codex-client-script true "/private/telemetry.py" "/witness/exec.jsonl")]
  (check "the ordinary Codex wrapper bytes remain the original direct exec"
         (= original (flow-capsule/codex-client-script false nil nil)))
  (check "the diagnostic wrapper records once then retains the original exec bytes"
         (and (str/starts-with? diagnostic header)
              (str/includes? diagnostic "FLOW_CAPSULE_EXEC_PID=$$ python3")
              (str/ends-with? diagnostic exec))))

;; A fresh private HOME is complete before start-capsule! invokes Herdr: the
;; preparation call immediately precedes Herdr's server process in that
;; function. The file is private and no operator HOME is touched.
(let [scratch (str (fs/create-temp-dir))
      private (str scratch "/private")
      private-home (str private "/home")
      state (str private "/state")
      operator-marker (str scratch "/operator-home-marker")
      saved-package @flow-capsule/package]
  (try
    (spit operator-marker "unchanged\n")
    (reset! flow-capsule/package flow-package)
    (fs/create-dirs state)
    (with-redefs [flow-capsule/private-root private
                  flow-capsule/home private-home
                  flow-capsule/state-root state
                  flow-capsule/ordinary-socket (str private "/runtime/ordinary.sock")]
      (flow-capsule/prepare-private-home-and-flow-wrapper!))
    (let [zshrc (fs/file private-home ".zshrc")]
      (check "private shell preparation writes the minimal zsh startup file"
             (= "# Flow capsule private shell.\n" (slurp zshrc)))
      (check "the private zsh startup file is owner-only"
             (= #{"OWNER_READ" "OWNER_WRITE"}
                (set (map str (fs/posix-file-permissions zshrc)))))
      (check "private shell preparation leaves the operator home marker unchanged"
             (= "unchanged\n" (slurp operator-marker))))
    (finally
      (reset! flow-capsule/package saved-package)
      (fs/delete-tree scratch))))

(let [request (str "Launch.{ { Mind test Terra } Predecessors.0 «" flow-capsule/brief "» }")]
  (check "the H0 diagnostic freezes the original exact Terra Launch request"
         (= request (flow-capsule/launch-query "Launch" flow-capsule/ho-seat)))
  (check "the H0 diagnostic selects exactly one scenario"
         (= 1 (count flow-capsule/h0-diagnostic-scenarios))))

(let [baseline #{"driver"}
      pending (flow-capsule/h0-discovered-ids baseline #{} #{"driver" "unlabelled-pending"} #{})
      retained (flow-capsule/h0-discovered-ids baseline pending #{"driver"} #{})]
  (check "an unlabelled pane appearing after the baseline is probed"
         (= #{"unlabelled-pending"} pending))
  (check "a disappeared or renamed agent keeps the same discovered pane for final probes"
         (= pending retained)))

(let [events (atom [])
      running (atom true)
      worker (future (Thread/sleep 100) (swap! events conj :inflight-finished))]
  (reset! flow-capsule/h0-diagnostic-sampler {:running running :worker worker})
  (with-redefs [flow-capsule/h0-sample! (fn [& _] (swap! events conj :final-snapshot))]
    (flow-capsule/stop-h0-sampler!))
  (swap! events conj :preservation-inventory-began)
  (Thread/sleep 150)
  (check "the sampler joins an in-flight probe before final snapshot and preservation"
         (= [:inflight-finished :final-snapshot :preservation-inventory-began] @events)))

(check "H0 diagnostic output never establishes the seven-line contract"
       (not (flow-capsule/seven-line-contract-complete? false true [{:pass true}])))

(let [scratch (str (fs/create-temp-dir))
      saved @flow-capsule/witness-dir]
  (try
    (reset! flow-capsule/witness-dir scratch)
    (with-redefs [flow-capsule/h0-herdr-command! (fn [& _] (throw (ex-info "probe failed" {})))]
      (flow-capsule/h0-sample!))
    (check "a sampler probe error is retained as evidence"
           (str/includes? (slurp (str scratch "/" flow-capsule/h0-sampler-record-name))
                          "sampler-error"))
    (let [receipt (with-redefs [flow-capsule/preserve-h0-jsonl-tree!
                                (fn [& _] (throw (ex-info "preservation failed" {})))]
                    (flow-capsule/preserve-h0-diagnostic!))]
      (check "a preservation error is recorded and returned without blocking teardown"
             (= "preservation failed" (:error (first (:files receipt))))))
    (finally (reset! flow-capsule/witness-dir saved) (fs/delete-tree scratch))))

;; Optional Claude state is absent before any Claude seat starts. Teardown
;; therefore reads no directory and finds no trace.
(let [scratch (str (fs/create-temp-dir))]
  (try
    (with-redefs [flow-capsule/claude-root (str scratch "/absent-claude")
                  flow-capsule/default-flows (str scratch "/absent-flows")
                  flow-capsule/capsule-flows (str scratch "/absent-capsule-flows")
                  flow-capsule/capsule-transcripts (str scratch "/absent-transcripts")]
      (check "traces accepts absent optional Claude state"
             (= [] (flow-capsule/traces #{}))))
    (finally (fs/delete-tree scratch))))

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
;; Nexus is not given DefaultMinutes, which it does not know; the replay to
;; the new Nexus is the same declarations with it.
(let [lines ["Configure.Nexus.{ /src 60 }"
             "Configure.Models.[ { Opus haiku } { Terra luna } ]"
             "Configure.DefaultMinutes.30"
             "Configure.LeaveOuts.[ { { Mind test Terra } Codex [ CodexAgentsMd ] } ]"
             (str "Configure.Metaflows.[ { { Psyche test Opus } None [ spirit:core ] [] Simplified Explicit.[] Maintained «» [] }"
                  " { { Psyche test Sonnet } None [ spirit:core ] [] Regular Default OnDemand «» [] }"
                  " { { Psyche test Haiku } None [ spirit:core ] [] Simplified Explicit.[] OnDemand «» [] }"
                  " { { Mind test Terra } None [ spirit:core ] [] Simplified Explicit.[] OnDemand «» [] } ]")]
      {:keys [old replay]} (flow-capsule/takeover-configuration lines)
      metaflows (first (filter #(clojure.string/starts-with? % "Configure.Metaflows.") old))]
  (check "the old Nexus is not given DefaultMinutes"
         (not-any? #(clojure.string/starts-with? % "Configure.DefaultMinutes.") old))
  (check "the replay carries every old declaration and DefaultMinutes, in the file's order"
         (= (map #(subs % 0 14) replay)
            ["Configure.Nexu" "Configure.Mode" "Configure.Defa" "Configure.Leav" "Configure.Meta"]))
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
                                   "{ { Mind test Sol } Codex [ CodexAgentsMd ] }")))

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
      lines (flow-capsule/rebind-lines panes flow-package #(or (clojure.string/ends-with? % "Haiku") (clojure.string/ends-with? % "Terra")))]
  (check "the trackers' stand-ins come first, then the rest in pane order"
         (= ["w3:p1" "w5:p1" "w2:p1" "w4:p1"] (map :pane lines)))
  (check "each line names its pane's FlowId and Address"
         (= {:pane "w2:p1" :flow-id "0a1b2c" :address "{ Psyche test Opus }"}
            (dissoc (nth lines 2) :line)))
  (check "the typed line carries the Rust-composed invocation to the new client"
         (= (str "! " flow-package "/bin/flow '{ " flow-capsule/ordinary-socket
                 " BindSeat.{ Psyche test Opus } }'")
            (:line (nth lines 2))))
  (let [{:keys [exit err]}
        (flow-capsule/run {} (str flow-package "/bin/flow")
                          "{ /run/user/1001/flow-testing/no-owner.sock Metaflows }")]
    (check "the packaged client accepts the helper's envelope before transport"
           (and (= 2 exit) (not (str/includes? err "invalid Flow invocation"))))))

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
              "Configure.Metaflows.[ { { Psyche test Opus } None [ spirit:core ] [] Simplified Explicit.[] Maintained «» [] } ]"]
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
             [:configure-only "Configure.Metaflows.[ { { Psyche test Opus } None [ spirit:core ] [] Simplified Explicit.[] Maintained «» [] } ]"]]
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
(let [metaflows (str "Configure.Metaflows.[ { { Psyche test Sonnet } None [ spirit:core ] [] Regular Default OnDemand «» [] }"
                     " { { Psyche test Haiku } None [ spirit:core ] [] Simplified Explicit.[] OnDemand «» [] } ]")]
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

(if (seq @failures)
  (do (println (str (count @failures) " failed")) (System/exit 1))
  (println "all passed"))
