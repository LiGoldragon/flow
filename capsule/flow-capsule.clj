;; flow-capsule: the real-world test of a Flow package in a capsule of its own.
;;
;; Usage: flow-capsule <flow-package> <source-root> [--keep-going] [--log <path>]
;;
;; The capsule owns: the runtime directory $XDG_RUNTIME_DIR/flow-testing/,
;; which is XDG_RUNTIME_DIR for every process it runs and holds the Flow and
;; Curriculum sockets; the state directory ~/.local/state/flow-testing/,
;; which holds the Flow store and the Curriculum's context checkouts; the
;; Herdr session flow-testing; its own Curriculum Nexus, built from the
;; Curriculum revision Flow pins; and the seats it starts. Every process it
;; runs gets a clean environment, so no inherited Herdr, Flow or Claude
;; identity leaks in.
;;
;; The Curriculum is configured as the running one is: its context
;; repositories are the origins the user's curriculum-nexus.service names,
;; read from its unit environment (read only), each cloned fresh at main
;; into the capsule; its records root is the source root's flows directory,
;; which holds one directory per flow (Curriculum reads <root>/<flow>/vision
;; and <root>/<flow>/notion).
;;
;; The Curriculum Flow pins (4493d1e) admits a module's dependencies only as
;; type:subject identities, and every context origin still names them
;; type-subject. The capsule rewrites those header lines in its own clones,
;; and commits the rewrite there, so the pinned Curriculum admits them; it
;; prints how many files it rewrote. Nothing else in a clone changes.
(ns flow-capsule
  (:require [babashka.fs :as fs]
            [babashka.process :as p]
            [cheshire.core :as json]
            [clojure.string :as str]))

;; ---------------------------------------------------------------- places

(def session "flow-testing")
(def home (System/getenv "HOME"))
(def runtime-root (or (System/getenv "XDG_RUNTIME_DIR") (str "/run/user/" (System/getenv "UID"))))
(def capsule-runtime (str runtime-root "/flow-testing"))
(def ordinary-socket (str capsule-runtime "/flow.sock"))
(def meta-socket (str capsule-runtime "/flow-meta.sock"))
(def state-root (str home "/.local/state/flow-testing"))
(def store-path (str state-root "/flow.sema"))
(def herdr-session-dir (str home "/.config/herdr/sessions/" session))
(def curriculum-socket (str capsule-runtime "/curriculum/curriculum.sock"))
(def curriculum-contexts (str state-root "/curriculum-context"))
(def curriculum-unit "curriculum-nexus.service")
(def libexec (str (fs/parent (fs/canonicalize (System/getProperty "babashka.file")))))
(def curriculum-package (str (fs/canonicalize (fs/file libexec "curriculum"))))

(def brief "Answer with one word and stop.")
(def quaternary "{ Psyche test Quaternary }")
(def secondary "{ Psyche test Secondary }")

;; ---------------------------------------------------------------- datom reading

(defn- string-end [text i]
  (let [n (count text) sb (StringBuilder.)]
    (loop [j (inc i)]
      (let [d (.charAt text j)]
        (cond
          (and (= d \\) (< (inc j) n) (= (.charAt text (inc j)) \»)) (do (.append sb \») (recur (+ j 2)))
          (= d \») [(str sb) (inc j)]
          :else (do (.append sb d) (recur (inc j))))))))

(defn tokenize [text]
  (let [n (count text)]
    (loop [i 0 out []]
      (if (>= i n)
        out
        (let [c (.charAt text i)]
          (cond
            (Character/isWhitespace c) (recur (inc i) out)
            (= c \;) (let [e (str/index-of text "\n" i)] (recur (if e (inc e) n) out))
            (#{\{ \} \[ \] \< \>} c) (recur (inc i) (conj out [:delim c]))
            (= c \«) (let [[s e] (string-end text i)] (recur e (conj out [:str s])))
            (= c \() (let [e (loop [j (inc i) depth 1]
                              (cond (zero? depth) j
                                    (>= j n) j
                                    :else (recur (inc j) (case (.charAt text j) \( (inc depth) \) (dec depth) depth))))]
                       (recur e (conj out [:str (subs text (inc i) (max (inc i) (dec e)))])))
            :else (let [e (loop [j i]
                            (if (and (< j n)
                                     (let [d (.charAt text j)]
                                       (not (or (Character/isWhitespace d) (#{\{ \} \[ \] \< \> \« \» \( \)} d)))))
                              (recur (inc j)) j))]
                    (recur e (conj out [:bare (subs text i e)])))))))))

(declare parse-one)

(defn- parse-run
  "A bare run whose leading capitalized segment ends in a period is a head:
  `Awake.abc123`, or `Listed.` followed by the structure it carries."
  [run toks]
  (if-let [[_ head rest-run] (re-matches #"([A-Z][A-Za-z0-9]*)\.(.*)" run)]
    (if (str/blank? rest-run)
      (let [[body toks] (parse-one toks)] [{:head head :body body} toks])
      (let [[body toks] (parse-run rest-run toks)] [{:head head :body body} toks]))
    [run toks]))

(defn- parse-one [toks]
  (let [[[kind v] & more] toks]
    (case kind
      :delim (let [close ({\{ \} \[ \] \< \>} v)]
               (loop [toks more items []]
                 (if (= (first toks) [:delim close])
                   [{:enclosure v :items items} (rest toks)]
                   (let [[item toks] (parse-one toks)] (recur toks (conj items item))))))
      :str [{:string v} more]
      :bare (parse-run v more)
      nil [nil nil])))

(defn read-datom [text]
  (try (first (parse-one (tokenize (str/trim (or text ""))))) (catch Exception _ nil)))

(defn head [d] (:head d))
(defn body [d] (:body d))
(defn items [d] (:items d))

(defn find-all
  "Every node under d that satisfies pred, depth first."
  [pred d]
  (cond
    (pred d) (cons d (mapcat #(find-all pred %) (cond (map? d) (concat (:items d) [(:body d)]) :else nil)))
    (map? d) (mapcat #(find-all pred %) (concat (:items d) (when (contains? d :body) [(:body d)])))
    :else nil))

(defn heads-named [name d] (find-all #(and (map? %) (= (:head %) name)) d))

(defn address-items [addr-text] (items (read-datom addr-text)))

;; ---------------------------------------------------------------- processes

(def package (atom nil))
(def source-root (atom nil))
(def driver-pane (atom nil))

(defn clean-env
  "The only environment any capsule process receives."
  ([] (clean-env {}))
  ([extra]
   (merge {"HOME" home
           "USER" (or (System/getenv "USER") "")
           "LOGNAME" (or (System/getenv "LOGNAME") (System/getenv "USER") "")
           "SHELL" (or (System/getenv "SHELL") "/bin/sh")
           "PATH" (str @package "/bin:" (System/getenv "PATH"))
           "XDG_RUNTIME_DIR" capsule-runtime
           "LANG" (or (System/getenv "LANG") "C.UTF-8")
           "TERM" "xterm-256color"}
          extra)))

(defn run
  "Runs a command to completion in the clean environment."
  [{:keys [env timeout] :or {env (clean-env) timeout 240000}} & args]
  (let [proc (p/process (vec args) {:env env :out :string :err :string :in ""})
        result (deref proc timeout ::timeout)]
    (if (= result ::timeout)
      (do (p/destroy-tree proc) {:exit -1 :out "" :err (str "timed out after " timeout " ms")})
      (select-keys result [:exit :out :err]))))

(defn herdr [& args]
  (apply run {} "herdr" "--session" session args))

(defn herdr-json [& args]
  (let [{:keys [out]} (apply herdr args)]
    (try (json/parse-string out true) (catch Exception _ nil))))

(defn marks
  "Herdr's pane marks: the Nexus places a caller by them, so every client
  call carries a pane of the capsule's own session."
  [pane] {"HERDR_SESSION" session "HERDR_PANE_ID" pane})

(defn reply-text [{:keys [exit out err]}]
  (let [out (str/trim out)]
    (if (and (zero? exit) (seq out)) out (str "exit " exit ": " (str/trim (str out " " err))))))

(defn flow
  "One ordinary-socket call; `pane` is the pane the caller stands in."
  ([datom] (flow datom @driver-pane))
  ([datom pane]
   (reply-text (run {:env (clean-env (merge (marks pane) {"FLOW_SOCKET" ordinary-socket}))}
                    (str @package "/bin/flow") datom))))

(defn flow-process
  "An ordinary-socket call started now and read later."
  [datom pane]
  (p/process [(str @package "/bin/flow") datom]
             {:env (clean-env (merge (marks pane) {"FLOW_SOCKET" ordinary-socket})) :out :string :err :string :in ""}))

(defn flow-meta [datom]
  (reply-text (run {:env (clean-env (merge (marks @driver-pane) {"FLOW_META_SOCKET" meta-socket}))}
                   (str @package "/bin/flow-meta") datom)))

;; ---------------------------------------------------------------- observing

(defn panes []
  (or (get-in (herdr-json "pane" "list") [:result :panes]) []))

(defn pane-ids [] (set (map :pane_id (panes))))

(defn pane-text [pane]
  (let [{:keys [exit out]} (herdr "pane" "read" pane "--source" "recent" "--lines" "400")]
    (when (zero? exit) out)))

(defn metaflows []
  (let [reply (flow "Metaflows")] {:reply reply :datom (read-datom reply)}))

(defn metaflow
  "The metaflow at an Address in a Listed reply: :state :awake/:asleep, :flow, :past, :queue."
  [listed addr]
  (let [want (map str (address-items addr))]
    (some (fn [m]
            (let [[a state past queue] (items m)]
              (when (= (map str (items a)) want)
                {:state (if (= (head state) "Awake") :awake :asleep)
                 :flow (when (= (head state) "Awake") (body state))
                 :past (vec (items past))
                 :queue (vec (items queue))})))
          (items (body listed)))))

(defn current [addr] (metaflow (:datom (metaflows)) addr))

(defn watch
  "Polls f every period until it answers truthy or the time is up; the last answer."
  [seconds f]
  (let [deadline (+ (System/currentTimeMillis) (* 1000 seconds))]
    (loop []
      (let [v (f)]
        (if (or v (> (System/currentTimeMillis) deadline)) v (do (Thread/sleep 1000) (recur)))))))

(defn inspect [addr] (flow-meta (str "Inspect." addr)))

(defn bound-pane
  "The Herdr pane an Inspected reply names for the metaflow's current flow."
  [inspected]
  (let [available (first (heads-named "Available" (read-datom inspected)))
        route (items (body available))]
    (when (= 4 (count route)) (str (nth route 2)))))

(defn native-session [inspected]
  (let [node (first (heads-named "Some" (second (items (body (read-datom inspected))))))]
    (some-> node body items second str)))

(defn transcript-first-user
  "The first user turn of a Claude transcript, as text."
  [native]
  (when native
    (when-let [file (first (fs/glob (str home "/.claude/projects") (str "*/" native ".jsonl")))]
      (some (fn [line]
              (let [entry (try (json/parse-string line true) (catch Exception _ nil))]
                (when (= "user" (:type entry))
                  (let [content (get-in entry [:message :content])]
                    (if (string? content) content (json/generate-string content))))))
            (str/split-lines (slurp (str file)))))))

(defn occurrences [text needle]
  (if text (count (re-seq (re-pattern (java.util.regex.Pattern/quote needle)) text)) 0))

;; ---------------------------------------------------------------- scenarios

(def results (atom []))
(def seen-flows (atom #{}))
(def keep-going (atom false))

(defn note-flows! []
  (let [listed (:datom (metaflows))]
    (doseq [m (items (body listed))]
      (let [[_ state past _] (items m)]
        (when (= (head state) "Awake") (swap! seen-flows conj (str (body state))))
        (doseq [f (items past)] (swap! seen-flows conj (str f)))))))

(defn print-line [{:keys [name reply pass ms watched]}]
  (println (str name " | " reply " | " (if pass "pass" "fail") " | " ms " ms | " watched))
  (flush))

(defn scenario
  "Runs one scenario; f answers {:reply :pass :watched}. Answers whether it passed."
  [name f]
  (let [start (System/currentTimeMillis)
        outcome (try (f) (catch Exception e {:reply (str "exception: " (.getMessage e)) :pass false :watched "the scenario threw"}))
        line (assoc outcome :name name :ms (- (System/currentTimeMillis) start))]
    (swap! results conj line)
    (print-line line)
    (try (note-flows!) (catch Exception _ nil))
    (:pass outcome)))

(defn launch [addr] (flow (str "Launch.{ " addr " Predecessors.0 «" brief "» }")))

(defn awake-flow [addr] (let [m (current addr)] (when (= :awake (:state m)) (:flow m))))

(defn sender-pane
  "The pane the Secondary seat lives in: a Send stands in it, so Flow names
  the Secondary as its sender."
  [] (bound-pane (inspect secondary)))

(defn send [delivery message pane]
  (flow (str "Send.{ Address." quaternary " " delivery " " message " }") pane))

(def scenarios
  [["0 sender seat: Launch { Psyche test Secondary } (Maintained)"
    (fn []
      (let [reply (launch secondary)
            awake (when (str/starts-with? reply "Started.") (watch 30 #(awake-flow secondary)))]
        {:reply reply :pass (boolean (and (str/starts-with? reply "Started.") awake))
         :watched (str "Metaflows: Secondary Awake as " awake "; it is the sender of every Send below")}))]

   ["1 launch Quaternary"
    (fn []
      (let [before (pane-ids)
            reply (launch quaternary)
            after (pane-ids)
            m (current quaternary)]
        {:reply reply
         :pass (boolean (and (str/starts-with? reply "Started.") (seq (remove before after)) (= :awake (:state m))))
         :watched (str "new panes in flow-testing " (vec (remove before after)) "; Metaflows Quaternary " (name (:state m)) " as " (:flow m))}))]

   ["2 OnDemand sleep"
    (fn []
      (let [flow-id (awake-flow quaternary)
            pane (bound-pane (inspect quaternary))
            asleep (watch 90 #(let [m (current quaternary)] (when (= :asleep (:state m)) m)))
            gone (not (contains? (pane-ids) pane))]
        {:reply (str "Metaflows Quaternary " (if asleep "Asleep" "still Awake") ", Past " (:past asleep))
         :pass (boolean (and asleep gone (some #{flow-id} (map str (:past asleep)))))
         :watched (str "flow " flow-id " pane " pane (if gone " closed" " still open") ", 90 s watch")}))]

   ["3 wake by Send"
    (fn []
      (let [old (last (map str (:past (current quaternary))))
            reply (send "Wake" (str "Soft.Order.«" brief " Tag s3.»") (sender-pane))
            woke (watch 60 #(awake-flow quaternary))
            m (current quaternary)]
        {:reply reply
         :pass (boolean (and (str/starts-with? reply "SendAccepted.") woke (not= (str woke) old)
                             (some #{old} (map str (:past m)))))
         :watched (str "Metaflows Quaternary Awake as " woke ", Past " (:past m))}))]

   ["4 Refresh.Address"
    (fn []
      (let [flow-id (str (awake-flow quaternary))
            pane (bound-pane (inspect quaternary))
            before (pane-text pane)
            reply (flow (str "Refresh." quaternary))
            after (pane-text pane)
            m (current quaternary)]
        {:reply reply
         :pass (boolean (and (str/starts-with? reply "Refreshed.") (str/includes? reply flow-id)
                             (= :asleep (:state m))
                             (or (nil? after) (<= (occurrences after "Tag s") (occurrences before "Tag s")))))
         :watched (str "old pane " pane (if after " still readable" " gone after the refresh")
                       "; Metaflows Quaternary " (name (:state m)))}))]

   ["5 Send during Refresh"
    (fn []
      (let [_ (send "Wake" (str "Soft.Order.«" brief " Tag s5a.»") (sender-pane))
            old (str (watch 60 #(awake-flow quaternary)))
            old-pane (bound-pane (inspect quaternary))
            refresh (flow-process (str "Refresh." quaternary) @driver-pane)
            sending (flow-process (str "Send.{ Address." quaternary " Wake Soft.Order.«" brief " Tag s5b.» }") (sender-pane))
            refresh-reply (str/trim (:out @refresh))
            send-reply (str/trim (:out @sending))
            old-text (pane-text old-pane)
            fresh (str (watch 60 #(let [f (awake-flow quaternary)] (when (and f (not= (str f) old)) f))))
            first-prompt (watch 30 #(transcript-first-user (native-session (inspect quaternary))))]
        {:reply (str "Refresh: " refresh-reply " / Send: " send-reply)
         :pass (boolean (and (str/starts-with? send-reply "SendAccepted.")
                             (seq fresh)
                             (str/includes? (str first-prompt) "Tag s5b")
                             (zero? (occurrences old-text "Tag s5b"))))
         :watched (str "old flow " old " pane " old-pane (if old-text " readable" " gone")
                       "; fresh flow " fresh " first prompt " (if (str/includes? (str first-prompt) "Tag s5b") "holds" "lacks") " Tag s5b")}))]

   ["6 Maintained Refresh"
    (fn []
      (let [old (str (awake-flow secondary))
            reply (flow (str "Refresh." secondary))
            m (current secondary)]
        {:reply reply
         :pass (boolean (and (str/starts-with? reply "Refreshed.") (= :awake (:state m))
                             (not= (str (:flow m)) old) (some #{old} (map str (:past m)))))
         :watched (str "Metaflows straight after: Secondary " (name (:state m)) " as " (:flow m) ", Past " (:past m))}))]

   ["7 Launch at an Awake metaflow"
    (fn []
      (let [old (str (awake-flow secondary))
            pane (bound-pane (inspect secondary))
            before (pane-text pane)
            last-seen (atom before)
            watcher (future (loop [] (when-let [t (pane-text pane)] (reset! last-seen t) (Thread/sleep 200) (recur))))
            reply (launch secondary)
            _ (deref watcher 15000 nil)
            m (current secondary)]
        {:reply reply
         :pass (boolean (and (str/starts-with? reply "Replaced.")
                             (= (occurrences @last-seen brief) (occurrences before brief))))
         :watched (str "predecessor " old " pane " pane " polled every 200 ms until gone: the brief appears "
                       (occurrences before brief) " times before, " (occurrences @last-seen brief)
                       " in its last reading; Secondary Awake as " (:flow m))}))]

   ["8 Inspect.Address"
    (fn []
      (let [reply (inspect secondary)]
        {:reply reply
         :pass (str/starts-with? reply "Inspected.")
         :watched "state, binding, queue, locks and attempts in one reply"}))]

   ["9 Withdraw"
    (fn []
      (let [asleep (watch 90 #(= :asleep (:state (current quaternary))))
            reply (send "Queue" (str "Soft.Order.«" brief " Tag s9.»") (sender-pane))
            delivery-id (some-> (read-datom reply) body items first str)
            queued (inspect quaternary)
            withdrawn (flow-meta (str "Withdraw.{ " quaternary " «" delivery-id "» }"))
            after (inspect quaternary)]
        {:reply (str "Send: " reply " / Withdraw: " withdrawn)
         :pass (boolean (and asleep (str/starts-with? withdrawn "Withdrawn.")
                             (str/includes? queued "Tag s9") (not (str/includes? after "Tag s9"))))
         :watched (str "Inspect before Withdraw " (if (str/includes? queued "Tag s9") "holds" "lacks")
                       " the request, after " (if (str/includes? after "Tag s9") "holds" "lacks") " it")}))]

   ["10 Rebind"
    (fn []
      (let [pane (sender-pane)
            unknown-flow (flow-meta (str "Rebind.{ " secondary " Flow.ffffff }"))
            absent-pane (flow-meta (str "Rebind.{ " secondary " Pane.«w999:p9» }"))
            own-pane (flow-meta (str "Rebind.{ " secondary " Pane.«" pane "» }"))]
        {:reply (str "Flow.ffffff: " unknown-flow " / Pane.w999:p9: " absent-pane " / Pane." pane ": " own-pane)
         :pass (boolean (and (str/includes? unknown-flow "FlowUnknown") (str/includes? absent-pane "PaneAbsent")
                             (str/starts-with? own-pane "Rebound.")))
         :watched "three Rebinds of the Secondary"}))]

   ["11 topic refusal"
    (fn []
      (let [before (pane-ids)
            reply (flow (str "Launch.{ { Psyche Test Quaternary } Predecessors.0 «" brief "» }"))]
        {:reply reply
         :pass (boolean (and (str/includes? reply "TopicRefused") (= before (pane-ids))))
         :watched "no pane opened"}))]

   ["12 Retire"
    (fn []
      (let [flow-id (str (awake-flow secondary))
            pane (sender-pane)
            reply (flow-meta (str "Retire." flow-id))
            gone (watch 10 #(not (contains? (pane-ids) pane)))
            m (watch 20 #(let [m (current secondary)] (when (not= (str (:flow m)) flow-id) m)))]
        {:reply reply
         :pass (boolean (and (str/starts-with? reply "FlowRetired.") gone))
         :watched (str "pane " pane (if gone " closed" " still open") "; Secondary afterwards "
                       (if m (str (name (:state m)) " " (:flow m)) "unchanged after 20 s"))}))]])

;; ---------------------------------------------------------------- capsule life

(def nexus (atom nil))
(def herdr-server (atom nil))
(def markers-before (atom #{}))

(defn flow-markers []
  (set (map str (fs/glob (str @source-root "/flows") ".*.flow-id" {:hidden true}))))

(defn refuse [why] (binding [*out* *err*] (println "flow-capsule:" why)) (System/exit 2))

(defn start-capsule! [log]
  (when (fs/exists? herdr-session-dir) (refuse (str "Herdr session " session " already exists; nothing started")))
  (when (fs/exists? capsule-runtime) (refuse (str capsule-runtime " already exists; nothing started")))
  (fs/delete-tree state-root)
  (fs/create-dirs state-root)
  (fs/create-dirs capsule-runtime)
  (reset! markers-before (flow-markers))
  (reset! herdr-server (p/process ["herdr" "--session" session "server"]
                                  {:env (clean-env) :out :discard :err :discard :in ""}))
  (when-not (watch 15 #(get-in (herdr-json "workspace" "list") [:result :workspaces]))
    (throw (ex-info "Herdr session did not answer" {})))
  (let [created (herdr-json "workspace" "create" "--cwd" @source-root "--label" "capsule-driver" "--no-focus")]
    (reset! driver-pane (get-in created [:result :root_pane :pane_id]))))

;; ---------------------------------------------------------------- curriculum

(def curriculum (atom nil))
(def curriculum-environment (atom nil))

(def context-roots
  "Each context repository as Curriculum's environment names its directory
  and origin."
  [["CURRICULUM_PSYCHES_REPOSITORY_DIR" "CURRICULUM_PSYCHES_REPOSITORY_URL" "psyche-context"]
   ["CURRICULUM_MIND_CONTEXT_DIR" "CURRICULUM_MIND_CONTEXT_URL" "mind-context"]
   ["CURRICULUM_FIELD_CONTEXT_DIR" "CURRICULUM_FIELD_CONTEXT_URL" "field-context"]])

(defn stable-curriculum-environment
  "The running Curriculum's configuration: its unit's environment, read
  through systemd and never written."
  []
  (let [{:keys [exit out err]} (run {:env (merge (clean-env) {"XDG_RUNTIME_DIR" runtime-root})}
                                    "systemctl" "--user" "show" curriculum-unit "-p" "Environment" "--value")]
    (when-not (zero? exit) (throw (ex-info "the running Curriculum's unit environment is unreadable" {:err err})))
    (into {} (keep #(let [[k v] (str/split % #"=" 2)] (when v [k v])) (str/split (str/trim out) #"\s+")))))

(def module-types #{"vision" "intent" "spirit" "knowledge" "inquiry" "compensation" "role" "raw"})

(defn colon-identity
  "A type-subject identity as type:subject; any other word as it was."
  [word]
  (let [[kind subject] (str/split word #"-" 2)]
    (if (and subject (module-types kind)) (str kind ":" subject) word)))

(defn colon-dependencies!
  "Rewrites every module's dependencies header line in one clone to
  type:subject identities, commits it in the clone, and answers how many
  files changed."
  [clone]
  (let [changed (atom 0)]
    (doseq [file (fs/glob clone "**.md")
            :when (not (str/includes? (str file) "/.git/"))]
      (let [text (slurp (str file))
            rewritten (str/replace text #"(?m)^dependencies: \[([^\]\n]*)\]"
                                   (fn [[_ inner]]
                                     (str "dependencies: ["
                                          (str/join ", " (map colon-identity (remove str/blank? (str/split inner #"[,\s]+"))))
                                          "]")))]
        (when (not= text rewritten)
          (spit (str file) rewritten)
          (swap! changed inc))))
    (when (pos? @changed)
      (let [git-env (clean-env {"GIT_CONFIG_GLOBAL" "/dev/null" "GIT_CONFIG_NOSYSTEM" "1"})
            {:keys [exit err]} (run {:env git-env} "git" "-C" clone "-c" "user.name=flow-capsule"
                                    "-c" "user.email=flow-capsule@localhost" "commit" "--quiet" "--all"
                                    "-m" "flow-capsule: dependencies as type:subject")]
        (when-not (zero? exit) (throw (ex-info (str "committing the rewrite in " clone " failed") {:err err})))))
    @changed))

(defn start-curriculum! [log]
  (let [stable (stable-curriculum-environment)
        layout (get stable "CURRICULUM_CONTEXT_LAYOUT")
        no-refresh (str (fs/which "true"))
        env (into (clean-env {"CURRICULUM_CONTEXT_LAYOUT" layout
                              "CURRICULUM_CONTEXT_REFRESH_PROGRAM" no-refresh
                              "CURRICULUM_RECORDS_DIR" (str @source-root "/flows")})
                  (for [[dir-key _ name] context-roots] [dir-key (str curriculum-contexts "/" name)]))]
    (fs/create-dirs curriculum-contexts)
    (doseq [[_ url-key name] context-roots]
      (let [url (get stable url-key)
            ;; The origin as named, anonymously: no user Git configuration
            ;; rewrites it to another transport or asks for credentials.
            {:keys [exit err]} (run {:env (clean-env {"GIT_CONFIG_GLOBAL" "/dev/null" "GIT_CONFIG_NOSYSTEM" "1"
                                                      "GIT_TERMINAL_PROMPT" "0"})}
                                    "git" "clone" "--quiet" "--depth" "1" "--single-branch" "--branch" "main"
                                    url (str curriculum-contexts "/" name))
            head (str/trim (:out (run {} "git" "-C" (str curriculum-contexts "/" name) "rev-parse" "HEAD")))]
        (when-not (zero? exit) (throw (ex-info (str "cloning " url " failed") {:err err})))
        (let [rewritten (colon-dependencies! (str curriculum-contexts "/" name))
              served (str/trim (:out (run {} "git" "-C" (str curriculum-contexts "/" name) "rev-parse" "HEAD")))]
          (println (str "curriculum context " name " | " url " main " head " | " rewritten
                        " files with dependencies rewritten to type:subject, served at " served)))))
    (reset! curriculum-environment env)
    ;; The Nexus serves only in a runtime directory that exists, as its unit's
    ;; RuntimeDirectory=curriculum (mode 0700) makes it; it opens none itself.
    (fs/create-dirs (fs/parent curriculum-socket))
    (fs/set-posix-file-permissions (fs/parent curriculum-socket) "rwx------")
    (reset! curriculum (p/process [(str curriculum-package "/bin/curriculum-nexus")]
                                  {:env env :dir @source-root :out :append :out-file (fs/file log)
                                   :err :append :err-file (fs/file log) :in ""}))
    (when-not (watch 30 #(or (fs/exists? curriculum-socket) (not (p/alive? @curriculum))))
      (throw (ex-info "the Curriculum Nexus opened no socket" {})))
    (when-not (p/alive? @curriculum)
      (throw (ex-info "the Curriculum Nexus exited" {:log (str/trim (slurp log))})))
    (let [probe (reply-text (run {:env env} (str curriculum-package "/bin/curriculum") "ResolveSkills.[ spirit:core intent:models ]"))]
      (println (str "curriculum | " curriculum-package " | layout " layout ", refresh " no-refresh
                    ", records " @source-root "/flows | socket " curriculum-socket " | ResolveSkills -> " probe)))))

(defn start-text []
  (str "{ «" ordinary-socket "» «" meta-socket "» «" store-path "» None }"))

(defn start-nexus! [log]
  (reset! nexus (p/process [(str @package "/bin/flow-nexus") (start-text)]
                           {:env (clean-env) :out :append :out-file (fs/file log) :err :append :err-file (fs/file log) :in ""}))
  (when-not (watch 20 #(and (fs/exists? ordinary-socket) (fs/exists? meta-socket)))
    (throw (ex-info "the Nexus opened no sockets" {:log (str/trim (slurp log))}))))

(defn listens?
  "Whether this exact process listens on the meta socket."
  [pid]
  (let [inodes (set (keep #(some->> (try (str (fs/read-link %)) (catch Exception _ nil)) (re-find #"socket:\[(\d+)\]") second)
                          (try (fs/list-dir (str "/proc/" pid "/fd")) (catch Exception _ []))))]
    (some (fn [row]
            (let [cols (str/split (str/trim row) #"\s+")]
              (and (= (get cols 7) meta-socket) (= (get cols 3) "00010000") (inodes (get cols 6)))))
          (rest (str/split-lines (try (String. (java.nio.file.Files/readAllBytes (fs/path (str "/proc/" pid "/net/unix")))) (catch Exception _ "")))))))

(defn configure-one!
  "One Configure through a configuration-only Nexus, which serves one request
  and exits. Its caller carries no pane marks: such a peer is refused there."
  [datom log]
  (let [child (p/process [(str @package "/bin/flow-nexus") "--configure-only" (start-text)]
                         {:env (clean-env) :out :append :out-file (fs/file log) :err :append :err-file (fs/file log) :in ""})
        pid (.pid (:proc child))]
    (when-not (watch 10 #(or (listens? pid) (not (p/alive? child))))
      (p/destroy child))
    (let [reply (if (p/alive? child)
                  (reply-text (run {:env (clean-env {"FLOW_META_SOCKET" meta-socket})} (str @package "/bin/flow-meta") datom))
                  (str "configuration-only Nexus exited before listening: " (str/trim (slurp log))))
          exit (:exit (deref child 15000 {:exit "still running"}))]
      (when (= exit "still running") (p/destroy child))
      [reply exit])))

(defn configure! [log]
  (let [file (fs/file libexec "configure.datom")]
    (doseq [line (str/split-lines (slurp file))
            :when (and (not (str/blank? line)) (not (str/starts-with? line ";")))]
      (let [datom (-> line (str/replace "@SOURCE_ROOT@" @source-root) (str/replace "@CAPSULE_RUNTIME@" capsule-runtime)
                      (str/replace "@CURRICULUM_SOCKET@" curriculum-socket))
            [reply exit] (configure-one! datom log)
            family (second (re-find #"^Configure\.([A-Za-z]+)" datom))
            ok (str/starts-with? reply "Configured.")]
        (println (str "configure " family " | " reply " | " (if ok "pass" "fail") " | configuration-only Nexus exit " exit))
        (flush)
        (when-not ok (throw (ex-info (str "Configure." family " refused") {:reply reply})))))))

(defn turns
  "The Haiku turns the capsule's flows took: the prompts each flow's harness
  reported submitted."
  []
  (into {} (for [f @seen-flows]
             (let [reply (flow-meta (str "ReadEvents." f))]
               [f {:prompts (count (heads-named "PromptSubmitted" (read-datom reply)))
                   :stops (count (heads-named "Stopped" (read-datom reply)))}]))))

(defn capsule-processes
  "Every process still carrying the capsule: its sockets, store or session."
  []
  (let [self (str (.pid (java.lang.ProcessHandle/current)))]
    (for [dir (fs/list-dir "/proc")
          :let [pid (str (fs/file-name dir))]
          :when (and (re-matches #"\d+" pid) (not= pid self))
          :let [cmd (try (str/replace (slurp (str dir "/cmdline")) "\u0000" " ") (catch Exception _ ""))
                env (try (slurp (str dir "/environ")) (catch Exception _ ""))]
          :when (or (str/includes? cmd capsule-runtime) (str/includes? cmd store-path)
                    (str/includes? cmd curriculum-package)
                    (str/includes? env (str "XDG_RUNTIME_DIR=" capsule-runtime "\u0000"))
                    (str/includes? cmd (str "--session " session))
                    (str/includes? env (str "FLOW_SOCKET=" ordinary-socket))
                    (str/includes? env (str "HERDR_SESSION=" session)))]
      (str pid " " (str/trim cmd)))))

(defn teardown! []
  (println "teardown")
  (let [started (System/currentTimeMillis)
        steps (atom [])]
    (when (and @nexus (fs/exists? meta-socket))
      (try (note-flows!) (catch Exception _ nil))
      (let [t (try (turns) (catch Exception _ {}))]
        (println (str "turns | " (pr-str t) " | total prompts " (reduce + (map :prompts (vals t)))
                      ", total stops " (reduce + (map :stops (vals t))))))
      (let [listed (try (:datom (metaflows)) (catch Exception _ nil))]
        (doseq [m (items (body listed))]
          (let [[a state] (items m)]
            (when (= (head state) "Awake")
              (swap! steps conj (str "Retire." (body state) " -> " (flow-meta (str "Retire." (body state))))))))))
    (doseq [w (get-in (herdr-json "workspace" "list") [:result :workspaces])]
      (swap! steps conj (str "close workspace " (:workspace_id w) " (" (:label w) ") -> " (str/trim (:out (herdr "workspace" "close" (:workspace_id w)))))))
    (when-let [n @nexus]
      (p/destroy n)
      (swap! steps conj (str "Nexus stopped, exit " (:exit (deref n 20000 {:exit "still running"})))))
    (when-let [c @curriculum]
      (p/destroy c)
      (swap! steps conj (str "Curriculum Nexus stopped, exit " (:exit (deref c 20000 {:exit "still running"})))))
    (when-let [h @herdr-server]
      (herdr "server" "stop")
      (when (= ::running (deref h 10000 ::running)) (p/destroy-tree h))
      (swap! steps conj (str "Herdr session " session " stopped")))
    (run {} "herdr" "session" "delete" session)
    (let [new-markers (remove @markers-before (flow-markers))
          ours (filter (fn [m] (some #(str/includes? m (str "." % ".flow-id")) @seen-flows)) new-markers)]
      (doseq [m ours] (fs/delete m))
      (swap! steps conj (str "flow-id claims removed " (count ours) " of " (count new-markers) " new")))
    (fs/delete-tree state-root)
    (fs/delete-tree capsule-runtime)
    (doseq [s @steps] (println (str "  " s)))
    (let [sessions (:out (run {} "herdr" "session" "list"))
          remaining {:herdr-session (str/includes? sessions session)
                     :processes (vec (or (watch 20 #(let [left (capsule-processes)] (when (empty? left) []))) (capsule-processes)))
                     :store (fs/exists? state-root)
                     :sockets (fs/exists? capsule-runtime)
                     :curriculum-process (boolean (some-> @curriculum p/alive?))
                     :curriculum-store (fs/exists? curriculum-contexts)
                     :curriculum-socket (fs/exists? curriculum-socket)
                     :claims (vec (filter (fn [m] (some #(str/includes? m (str "." % ".flow-id")) @seen-flows)) (flow-markers)))}
          clean (and (not (:herdr-session remaining)) (empty? (:processes remaining)) (not (:store remaining))
                     (not (:sockets remaining)) (empty? (:claims remaining))
                     (not (:curriculum-process remaining)) (not (:curriculum-store remaining))
                     (not (:curriculum-socket remaining)))]
      (print-line {:name "13 teardown" :reply (pr-str remaining) :pass clean
                   :ms (- (System/currentTimeMillis) started)
                   :watched "Herdr session list; /proc cmdline and environ, waiting up to 20 s for capsule processes to end; store, sockets, flow-id claims; the Curriculum Nexus process, its context checkouts and socket"})
      clean)))

(defn -main [& args]
  (let [[pkg root & options] args]
    (when-not (and pkg root) (refuse "usage: flow-capsule <flow-package> <source-root> [--keep-going] [--log <path>]"))
    (reset! package (str (fs/canonicalize pkg)))
    (reset! source-root (str (fs/canonicalize root)))
    (reset! keep-going (boolean (some #{"--keep-going"} options)))
    (let [log (or (second (drop-while #(not= % "--log") options)) (str state-root "/nexus.log"))
          wall (System/currentTimeMillis)
          passed (atom true)]
      (println (str "flow-capsule " @package " | " (str/trim (:out (run {} (str @package "/bin/flow-nexus") "--version")))
                    " | curriculum " curriculum-package))
      (try
        (start-capsule! log)
        (start-curriculum! log)
        (configure! log)
        (start-nexus! log)
        (println (str "configuration | " (flow-meta "Configuration")))
        (loop [[[name f] & more] scenarios]
          (when name
            (let [ok (scenario name f)]
              (when-not ok (reset! passed false))
              (if (or ok @keep-going) (recur more) (println "stopped after a failure")))))
        (catch Exception e
          (reset! passed false)
          (println (str "stopped: " (.getMessage e) " " (pr-str (ex-data e))))))
      (let [clean (teardown!)]
        (println (str "wall " (- (System/currentTimeMillis) wall) " ms | "
                      (count (filter :pass @results)) " of " (count @results) " scenarios passed"))
        (System/exit (if (and @passed clean) 0 1))))))

(when (= *file* (System/getProperty "babashka.file"))
  (apply -main *command-line-args*))
