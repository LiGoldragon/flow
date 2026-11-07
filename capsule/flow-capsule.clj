;; flow-capsule: the real-world test of a Flow package in a capsule of its own.
;;
;; Usage: flow-capsule <flow-package> [--workspace-only | --handover | --handover-h0-diagnostic] [--keep-going] [--log <path>]
;;        flow-capsule --takeover <old flow package> <new flow package> [--log <path>]
;;
;; --handover runs only the handover canary (the handover section) after the
;; capsule comes up, then tears it down.
;;
;; --takeover runs the bie3.6 takeover and rollback instead of the
;; scenarios (see the takeover section); FLOW_CAPSULE_SKIP_CODEX=1 skips its
;; Codex seats as it skips the Codex scenarios.
;;
;; A run keeps its raw Nexus log and every flow transcript past teardown,
;; copied before the state root is deleted into
;; <witness root>/capsule-<flow version>-<timestamp>/. The witness root is
;; FLOW_CAPSULE_WITNESS_DIR, else $HOME/witnesses/<flow id>, the flow id
;; FLOW_ID or else the name of the calling flow's FLOW_DIRECTORY; without a
;; home or a flow id the capsule starts nothing.
;;
;; The capsule owns: the runtime directory $XDG_RUNTIME_DIR/flow-testing/,
;; which is XDG_RUNTIME_DIR for every process it runs and holds the Flow and
;; Curriculum sockets; the state directory ~/.local/state/flow-testing/,
;; which holds the Flow store and the Curriculum's context checkouts; the
;; source root $TMPDIR/flow-testing/source; the Herdr session flow-testing; its own Curriculum
;; Nexus, built from the Curriculum revision Flow pins; the seats it starts
;; and the Claude transcripts and session environments they leave. Every
;; process it runs gets a clean environment, so no inherited Herdr, Flow or
;; Claude identity leaks in.
;;
;; The source root is an empty directory of the capsule's: Flow's
;; Configure.Nexus names it, its seats run in it, and its flows directory
;; holds their flow-id claims. That flows directory is also the Curriculum's
;; records root (Curriculum reads <root>/<flow>/vision and
;; <root>/<flow>/notion), so it reads no flow but the capsule's. It lies
;; in the temporary directory because Claude blocks a seat's startup on a
;; trust question in a working directory not inside one it trusts, and the
;; temporary directory is one the user's Claude trusts.
;;
;; The Curriculum is configured as the running one is otherwise: its context
;; repositories are the origins the user's curriculum-nexus.service names,
;; read from its unit environment (read only), each cloned fresh at main
;; into the capsule.
(ns flow-capsule
  (:require [babashka.fs :as fs]
            [babashka.process :as p]
            [cheshire.core :as json]
            [clojure.string :as str]))

;; ---------------------------------------------------------------- places

(def h0-diagnostic-mode (boolean (some #{"--handover-h0-diagnostic"} *command-line-args*)))
(def handover-mode (boolean (some #{"--handover" "--handover-h0-diagnostic"} *command-line-args*)))
(def private-root "/tmp/flow-0532-01e4a4-capsule")
(def operator-home (System/getenv "HOME"))
(def session "flow-testing")
(def home (if handover-mode (str private-root "/home") operator-home))
(def runtime-root (or (System/getenv "XDG_RUNTIME_DIR") (str "/run/user/" (System/getenv "UID"))))
(def capsule-runtime (if handover-mode (str private-root "/runtime") (str runtime-root "/flow-testing")))
(def ordinary-socket (str capsule-runtime "/flow.sock"))
(def meta-socket (str capsule-runtime "/flow-meta.sock"))
(def state-root (if handover-mode (str private-root "/state") (str home "/.local/state/flow-testing")))
(def store-path (str state-root "/flow.sema"))
(def config-home (str state-root "/config"))
;; The capsule's own XDG_CONFIG_HOME: Herdr reads its config and keeps its
;; sessions there, so the capsule's Herdr shares nothing with the user's.
(def herdr-session-dir (str config-home "/herdr/sessions/" session))
(def h0-wrapper-record-name "handover-H0-exec-wrapper.jsonl")
(def h0-sampler-record-name "handover-H0-herdr-sampler.jsonl")
(def h0-herdr-stdout (str state-root "/herdr-server.stdout.log"))
(def h0-herdr-stderr (str state-root "/herdr-server.stderr.log"))
;; The capsule's own Codex app-server: a private CODEX_HOME holding only a
;; copy of the user's Codex login, a private control socket, and a client
;; that runs Codex in that home. The Codex executable, its model and the
;; login it copies are taken from the environment where given.
(def codex-home (if handover-mode (str private-root "/codex-stable") (str state-root "/codex-home")))
(def codex-socket (if handover-mode (str private-root "/runtime/stable.sock") (str capsule-runtime "/codex/app-server-control.sock")))
(def codex-client (if handover-mode (str private-root "/bin/codex-stable") (str state-root "/codex-client")))
(def codex-binary (or (System/getenv "FLOW_CAPSULE_CODEX") (some-> (fs/which "codex") str)))
(def codex-model (or (System/getenv "FLOW_CAPSULE_CODEX_MODEL") "gpt-5.6-luna"))
(def codex-login (or (System/getenv "FLOW_CAPSULE_CODEX_LOGIN") (str operator-home "/.codex-next/auth.json")))
(def curriculum-socket (str capsule-runtime "/curriculum/curriculum.sock"))
(def curriculum-contexts (str state-root "/curriculum-context"))
(def source-parent (if handover-mode private-root (str (fs/path (System/getProperty "java.io.tmpdir") "flow-testing"))))
(def source-root (str source-parent "/source"))
(def workspace-a (str source-parent "/workspace-a"))
(def workspace-b (str source-parent "/workspace-b"))
(def invalid-workspace (str source-parent "/workspace-absent"))
(def capsule-flows (str source-root "/flows"))
;; Flow's default source root's flows directory, where the running Flow keeps
;; its flows: the verdict checks that none of the capsule's flows is there.
(def default-flows (str operator-home "/primary/flows"))
(def claude-root (str home "/.claude"))
;; Claude names a project's transcript directory by its working directory,
;; every character but a letter or digit written as a dash.
(def capsule-transcripts (str claude-root "/projects/" (str/replace source-root #"[^A-Za-z0-9]" "-")))
(def curriculum-unit "curriculum-nexus.service")
(def libexec (str (fs/parent (fs/canonicalize (System/getProperty "babashka.file")))))
(def curriculum-package (str (fs/canonicalize (fs/file libexec "curriculum"))))
(def codex-observer (str (fs/canonicalize (fs/file libexec "codex-observer.py"))))

(def brief (if handover-mode
  "Controlled isolated canary. Execute shell commands only in explicit OPERATOR-SCRIPT prompts. For reminder notices containing Keep or Drop examples, answer seen and wait for the operator; do not execute those examples. Otherwise answer one word and stop."
  "Answer with one word and stop."))
(def quaternary "{ Psyche test Haiku }")
(def secondary "{ Psyche test Opus }")
(def tertiary-address "{ Psyche test Sonnet }")

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
      ;; A period before a closer, or at the end, ends a word (a seat's
      ;; last message, `Acknowledged.`): it carries nothing.
      (if (let [[kind v] (first toks)] (or (nil? kind) (and (= kind :delim) (#{\} \] \>} v))))
        [run toks]
        (let [[body toks] (parse-one toks)] [{:head head :body body} toks]))
      (let [[body toks] (parse-run rest-run toks)] [{:head head :body body} toks]))
    [run toks]))

(defn- parse-one [toks]
  (let [[[kind v] & more] toks]
    (case kind
      :delim (let [close ({\{ \} \[ \] \< \>} v)]
               (loop [toks more items []]
                 (if (or (empty? toks) (= (first toks) [:delim close]))
                   [{:enclosure v :items items} (rest toks)]
                   (let [[item toks] (parse-one toks)] (recur toks (conj items item))))))
      :str [{:string v} more]
      :bare (parse-run v more)
      nil [nil nil])))

(defn read-datom [text]
  (try (first (parse-one (tokenize (str/trim (or text ""))))) (catch Exception _ nil)))

(def package-interface (atom {}))

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
(def driver-pane (atom nil))
(def capsule-ownership
  "Resources this invocation actually acquired. Teardown acts only on these
  milestones, so a startup refusal cannot remove a pre-existing capsule."
  (atom #{}))

(defn cleanup-plan
  "Pure cleanup decisions for an acquired-resource set."
  [owned]
  {:herdr-session (contains? owned :herdr-session)
   :traces (contains? owned :source-parent)
   :state-root (contains? owned :state-root)
   :source-parent (contains? owned :source-parent)
   :runtime (contains? owned :runtime)})

(defn clean-env
  "The only environment any capsule process receives."
  ([] (clean-env {}))
  ([extra]
   (merge {"HOME" home
           "USER" (or (System/getenv "USER") "")
           "LOGNAME" (or (System/getenv "LOGNAME") (System/getenv "USER") "")
           "SHELL" (or (System/getenv "SHELL") "/bin/sh")
           "PATH" (str (when handover-mode (str private-root "/bin:")) @package "/bin:" (System/getenv "PATH"))
           "XDG_RUNTIME_DIR" capsule-runtime
           "XDG_CONFIG_HOME" config-home
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

(defn client-argument [pkg client socket query]
  (case (get @package-interface pkg)
    :query query
    :invocation (let [composer (str pkg "/bin/" client "-compose-invocation")
                      {:keys [exit out err]} (run {} composer socket query)]
                  (if (zero? exit)
                    (str/trim out)
                    (throw (ex-info "typed client invocation composition failed"
                                    {:package pkg :client client :query query :error (str/trim err)}))))
    (throw (ex-info "package client interface is undeclared" {:package pkg}))))

(defn herdr [& args]
  (apply run {} "herdr" "--session" session args))

(defn herdr-json [& args]
  (let [{:keys [out]} (apply herdr args)]
    (try (json/parse-string out true) (catch Exception _ nil))))

(def h0-diagnostic-record-lock (Object.))
(def h0-diagnostic-sampler (atom nil))
(def h0-baseline-pane-ids (atom #{}))
(def h0-discovered-pane-ids (atom #{}))
(declare witness-dir)

(defn append-h0-json! [name value]
  (when-let [dir @witness-dir]
    (locking h0-diagnostic-record-lock
      (fs/create-dirs dir)
      (let [path (str dir "/" name)]
        (spit path (str (json/generate-string value) "\n") :append true)
        (fs/set-posix-file-permissions path "rw-------")))))

(defn h0-herdr-command! [label & args]
  (let [result (apply run {:timeout 2000} "herdr" "--session" session args)]
    (append-h0-json! h0-sampler-record-name
                     (merge {:time (str (java.time.Instant/now)) :kind "herdr"
                             :label label :arguments (vec args)} result))
    result))

(defn h0-matching-pane-ids [text]
  (let [parsed (try (json/parse-string (or text "") true) (catch Exception _ nil))]
    (set (keep (fn [pane]
                 (when (some #(str/includes? (str %) "Mind test Terra") (vals pane))
                   (some-> (:pane_id pane) str)))
               (get-in parsed [:result :panes])))))

(defn h0-matching-agent-pane-ids [text]
  (let [parsed (try (json/parse-string (or text "") true) (catch Exception _ nil))
        agents (or (get-in parsed [:result :agents]) (:result parsed))]
    (set (keep (fn [agent]
                 (when (and (map? agent)
                            (some #(str/includes? (str %) "Mind test Terra") (vals agent)))
                   (some-> (or (:pane_id agent) (:pane agent)) str)))
               (when (sequential? agents) agents)))))

(defn h0-pane-ids [text]
  (let [parsed (try (json/parse-string (or text "") true) (catch Exception _ nil))]
    (set (keep #(some-> (:pane_id %) str) (get-in parsed [:result :panes])))))

(defn h0-agent-pane-ids [text]
  (let [parsed (try (json/parse-string (or text "") true) (catch Exception _ nil))
        agents (or (get-in parsed [:result :agents]) (:result parsed))]
    (set (keep #(when (map? %) (some-> (or (:pane_id %) (:pane %)) str))
               (when (sequential? agents) agents)))))

(defn h0-discovered-ids [baseline retained current labelled]
  (into (set retained) (concat (remove (set baseline) current) labelled)))

(defn h0-process-stat! [pid]
  (let [path (str "/proc/" pid "/stat")]
    (try
      (let [stat (slurp path)
            close (.lastIndexOf stat ")")
            fields (str/split (str/trim (subs stat (inc close))) #"\s+")]
        (append-h0-json! h0-sampler-record-name
                         {:time (str (java.time.Instant/now)) :kind "process-stat"
                          :pid pid :state (first fields) :starttime (nth fields 19 nil)}))
      (catch Exception e
        (append-h0-json! h0-sampler-record-name
                         {:time (str (java.time.Instant/now)) :kind "process-stat"
                          :pid pid :state "gone" :error (.getMessage e)})))))

(defn h0-recorded-pids []
  (let [path (str @witness-dir "/" h0-wrapper-record-name)]
    (set (keep (fn [line]
                 (try (some-> (json/parse-string line true) :pid long)
                      (catch Exception _ nil)))
               (when (fs/exists? path) (str/split-lines (slurp path)))))))

(defn h0-sample!
  ([] (h0-sample! (constantly true)))
  ([continue?]
  (try
    (let [panes (h0-herdr-command! "pane-list" "pane" "list")
          agents (h0-herdr-command! "agent-list" "agent" "list")
          current (into (h0-pane-ids (:out panes)) (h0-agent-pane-ids (:out agents)))
          labelled (into (h0-matching-pane-ids (:out panes))
                         (h0-matching-agent-pane-ids (:out agents)))
          ids (swap! h0-discovered-pane-ids
                     #(h0-discovered-ids @h0-baseline-pane-ids % current labelled))]
      (doseq [pane ids]
        (when (continue?) (h0-herdr-command! "pane-get" "pane" "get" pane))
        (when (continue?) (h0-herdr-command! "pane-process-info" "pane" "process-info" "--pane" pane))
        (when (continue?)
          (h0-herdr-command! "pane-read" "pane" "read" pane "--source" "recent-unwrapped"
                             "--lines" "200" "--format" "text"))
        (when (continue?) (h0-herdr-command! "agent-get" "agent" "get" pane))
        (when (continue?)
          (h0-herdr-command! "agent-explain" "agent" "explain" pane "--json" "--verbose")))
      (doseq [pid (h0-recorded-pids) :while (continue?)] (h0-process-stat! pid)))
    (catch Exception e
      (append-h0-json! h0-sampler-record-name
                       {:time (str (java.time.Instant/now)) :kind "sampler-error"
                        :error (.getMessage e)})))))

(defn start-h0-sampler! []
  (let [panes (h0-herdr-command! "baseline-pane-list" "pane" "list")
        agents (h0-herdr-command! "baseline-agent-list" "agent" "list")
        baseline (into (h0-pane-ids (:out panes)) (h0-agent-pane-ids (:out agents)))
        _ (reset! h0-baseline-pane-ids baseline)
        _ (reset! h0-discovered-pane-ids #{})
        _ (append-h0-json! h0-sampler-record-name
                           {:time (str (java.time.Instant/now)) :kind "baseline-pane-ids"
                            :paneIds (vec (sort baseline))})
        running (atom true)
        worker (future
                 (while @running
                   (Thread/sleep 500)
                   (when @running (h0-sample! #(deref running)))))]
    (reset! h0-diagnostic-sampler {:running running :worker worker})))

(defn stop-h0-sampler! []
  (when-let [{:keys [running worker]} @h0-diagnostic-sampler]
    (reset! running false)
    ;; Each command is bounded and the running predicate is checked between
    ;; probes. Join without overlap: preservation begins only after the worker
    ;; has actually ended.
    (deref worker))
  (h0-sample!)
  (reset! h0-diagnostic-sampler nil))

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
   (reply-text (run {:env (clean-env (marks pane))}
                    (str @package "/bin/flow")
                    (client-argument @package "flow" ordinary-socket datom)))))

(defn flow-process
  "An ordinary-socket call started now and read later."
  [datom pane]
  (p/process [(str @package "/bin/flow") (client-argument @package "flow" ordinary-socket datom)]
             {:env (clean-env (marks pane)) :out :string :err :string :in ""}))

(defn flow-meta [datom]
  (reply-text (run {:env (clean-env (marks @driver-pane))}
                   (str @package "/bin/flow-meta")
                   (client-argument @package "flow-meta" meta-socket datom))))

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

(defn verified-awake-current-from
  "The exact Address's Awake snapshot only when its queue is structurally a
  vector. Missing, asleep, and malformed observations are explicit errors."
  [listed addr]
  (let [want (mapv str (address-items addr))
        match (some (fn [m]
                      (let [[a state past queue :as fields] (items m)]
                        (when (= (mapv str (items a)) want)
                          {:fields fields :state state :past past :queue queue})))
                    (items (body listed)))
        {:keys [fields state past queue]} match]
    (when-not match
      (throw (ex-info "exact Address absent from Metaflows" {:address addr})))
    (when-not (and (= 4 (count fields)) (= "Awake" (head state)) (some? (body state)))
      (throw (ex-info "exact Address is not a well-formed Awake snapshot" {:address addr :state state})))
    (when-not (and (= \[ (:enclosure past)) (= \[ (:enclosure queue)))
      (throw (ex-info "exact Address has a malformed Past or queue" {:address addr :past past :queue queue})))
    {:state :awake :flow (str (body state)) :past (vec (items past)) :queue (vec (items queue))}))

(defn verified-awake-current [addr]
  (let [{:keys [reply datom]} (metaflows)]
    (when-not datom
      (throw (ex-info "Metaflows observation was unavailable" {:address addr :reply reply})))
    (verified-awake-current-from datom addr)))

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

(defn bound-process-id
  "The native process pid exposed by Inspect.Address for its current binding."
  [inspected]
  (let [[_ binding _ _] (items (body (read-datom inspected)))
        [_ process-option] (items binding)]
    (when (= "Some" (head process-option))
      (some-> process-option body items first str parse-long))))

(defn process-cwd [pid]
  (when pid
    (try (str (fs/canonicalize (str "/proc/" pid "/cwd")))
         (catch Exception _ nil))))

(defn native-processes
  "Live processes carrying one flow identity, with their independently read cwd."
  [flow-id]
  (vec
   (keep (fn [dir]
           (let [pid (str (fs/file-name dir))
                 env (try (slurp (str dir "/environ")) (catch Exception _ ""))]
             (when (and (re-matches #"\d+" pid)
                        (str/includes? env (str "FLOW_ID=" flow-id "\u0000")))
               {:pid (parse-long pid) :cwd (process-cwd pid)})))
         (fs/list-dir "/proc"))))

(defn native-cwd [flow-id]
  (let [roots (set (keep :cwd (native-processes flow-id)))]
    (when (= 1 (count roots)) (first roots))))

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

(defn print-line [{:keys [name reply pass skipped ms watched]}]
  (println (str name " | " reply " | " (cond skipped "SKIPPED" pass "pass" :else "fail") " | " ms " ms | " watched))
  (flush))

(defn scenario
  "Runs one scenario; f answers {:reply :pass :watched}. Answers whether it passed."
  [name f]
  (let [start (System/currentTimeMillis)
        outcome (try
                  (f)
                  (catch Exception e
                    (let [typed (select-keys (ex-data e) [:package :client :query :error
                                                          :prerequisite :identifier :address :h0-refusals])]
                      {:reply (str "exception: " (.getMessage e)
                                   (when (seq typed) (str " " (pr-str typed))))
                       :pass false
                       :watched "the scenario threw"})))
        line (assoc outcome :name name :ms (- (System/currentTimeMillis) start))]
    (swap! results conj line)
    (print-line line)
    (try (note-flows!) (catch Exception _ nil))
    (boolean (or (:pass outcome) (:skipped outcome)))))

(defn on-demand-secondary
  "The capsule's Configure.Metaflows with the Opus declared OnDemand."
  []
  (let [line (first (filter #(str/starts-with? % "Configure.Metaflows.")
                            (str/split-lines (slurp (fs/file libexec (if handover-mode "configure-handover.datom" "configure.datom"))))))]
    (str/replace-first line "Simplified Explicit.[] Maintained" "Simplified Explicit.[] OnDemand")))

(defn workspace-configuration [workspaces]
  (let [line (first (filter #(str/starts-with? % "Configure.Metaflows.")
                            (str/split-lines (slurp (fs/file libexec (if handover-mode "configure-handover.datom" "configure.datom"))))))]
    (reduce (fn [configuration [address root]]
              (str/replace-first configuration
                                 (str "{ " address " None ")
                                 (str "{ " address " Some.«" root "» ")))
            line
            workspaces)))

(defn secondary-workspace-configuration [root]
  (workspace-configuration [[secondary root]]))

(defn launch-query [head addr]
  (str head ".{ " addr " Predecessors.0 «" brief "» }"))

(defn launch [addr] (flow (launch-query "Launch" addr)))

(defn awake-flow [addr] (let [m (current addr)] (when (= :awake (:state m)) (:flow m))))

(defn sender-pane
  "The pane the Opus seat lives in: a Send stands in it, so Flow names
  the Opus as its sender."
  [] (bound-pane (inspect secondary)))

(defn send [delivery message pane]
  (flow (str "Send.{ " quaternary " " delivery " " message " }") pane))

(defn send-to [addr delivery message pane]
  (flow (str "Send.{ " addr " " delivery " " message " }") pane))

(declare nexus nexus-log start-nexus!)

(def base-scenarios
  [["0 sender seat: Launch { Psyche test Opus } (Maintained)"
    (fn []
      (let [reply (launch secondary)
            awake (when (str/starts-with? reply "Started.") (watch 30 #(awake-flow secondary)))]
        {:reply reply :pass (boolean (and (str/starts-with? reply "Started.") awake))
         :watched (str "Metaflows: Opus Awake as " awake "; it is the sender of every Send below")}))]

   ["0a workspace: default, continuation, invalid replacement, updated replacement"
    (fn []
      (let [initial-flow (str (awake-flow secondary))
            initial-processes (native-processes initial-flow)
            initial-cwd (native-cwd initial-flow)
            configured-a (flow-meta (secondary-workspace-configuration workspace-a))
            replaced-a (launch secondary)
            flow-a (str (awake-flow secondary))
            processes-a (native-processes flow-a)
            cwd-a (native-cwd flow-a)
            configured-invalid (flow-meta (secondary-workspace-configuration invalid-workspace))
            n @nexus
            _ (p/destroy n)
            stopped (:exit (deref n 20000 {:exit "still running"}))
            _ (fs/delete-if-exists ordinary-socket)
            _ (fs/delete-if-exists meta-socket)
            _ (start-nexus! @nexus-log)
            continued-flow (str (awake-flow secondary))
            continued-processes (native-processes continued-flow)
            continued-cwd (native-cwd continued-flow)
            invalid-replacement (launch secondary)
            after-invalid-flow (str (awake-flow secondary))
            after-invalid-cwd (native-cwd after-invalid-flow)
            configured-b (flow-meta (workspace-configuration [[secondary workspace-b]
                                                               [quaternary workspace-a]]))
            replaced-b (launch secondary)
            flow-b (str (awake-flow secondary))
            processes-b (native-processes flow-b)
            cwd-b (native-cwd flow-b)
            started-a (launch quaternary)
            flow-other-a (str (awake-flow quaternary))
            cwd-other-a (native-cwd flow-other-a)
            started-default (launch tertiary-address)
            flow-default (str (awake-flow tertiary-address))
            cwd-default (native-cwd flow-default)
            record-files (filter fs/regular-file? (file-seq (fs/file capsule-flows)))
            records-stay (and (seq record-files)
                              (every? #(str/starts-with? (str %) (str source-root "/flows/"))
                                      record-files))]
        {:reply (str "A: " configured-a " / " replaced-a
                     " / invalid: " configured-invalid " / restart " stopped
                     " / replace: " invalid-replacement
                     " / B: " configured-b " / " replaced-b)
         :pass (boolean (and (= initial-cwd source-root)
                             (str/starts-with? configured-a "Configured.")
                             (str/starts-with? replaced-a "Replaced.")
                             (seq initial-processes) (not= initial-flow flow-a)
                             (seq processes-a) (= cwd-a workspace-a)
                             (str/starts-with? configured-invalid "Configured.")
                             (= continued-flow flow-a) (= (set continued-processes) (set processes-a))
                             (= continued-cwd workspace-a)
                             (str/includes? invalid-replacement "CompositionRefused.Unconfigured")
                             (= after-invalid-flow flow-a) (= after-invalid-cwd workspace-a)
                             (str/starts-with? configured-b "Configured.")
                             (str/starts-with? replaced-b "Replaced.")
                             (not= flow-b flow-a) (seq processes-b)
                             (= cwd-b workspace-b)
                             (str/starts-with? started-a "Started.") (= cwd-other-a workspace-a)
                             (str/starts-with? started-default "Started.") (= cwd-default source-root)
                             records-stay))
         :watched (str "default flow " initial-flow " processes " initial-processes " cwd " initial-cwd
                       "; A flow " flow-a " processes " processes-a " cwd " cwd-a
                       "; after invalid declaration and Nexus restart flow " continued-flow
                       " processes " continued-processes " cwd " continued-cwd
                       "; refused replacement kept " after-invalid-flow " cwd " after-invalid-cwd
                       "; simultaneous B flow " flow-b " processes " processes-b " cwd " cwd-b
                       ", A flow " flow-other-a " cwd " cwd-other-a
                       ", default flow " flow-default " cwd " cwd-default
                       "; records remain under " source-root " " records-stay)}))]

   ["1 launch Haiku"
    (fn []
      (let [before (pane-ids)
            reply (launch quaternary)
            after (pane-ids)
            m (current quaternary)]
        {:reply reply
         :pass (boolean (and (str/starts-with? reply "Started.") (seq (remove before after)) (= :awake (:state m))))
         :watched (str "new panes in flow-testing " (vec (remove before after)) "; Metaflows Haiku " (name (:state m)) " as " (:flow m))}))]

   ["2 OnDemand sleep"
    (fn []
      (let [flow-id (awake-flow quaternary)
            pane (bound-pane (inspect quaternary))
            asleep (watch 90 #(let [m (current quaternary)] (when (= :asleep (:state m)) m)))
            gone (not (contains? (pane-ids) pane))]
        {:reply (str "Metaflows Haiku " (if asleep "Asleep" "still Awake") ", Past " (:past asleep))
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
         :watched (str "Metaflows Haiku Awake as " woke ", Past " (:past m))}))]

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
                       "; Metaflows Haiku " (name (:state m)))}))]

   ["5 Send during Refresh"
    (fn []
      (let [_ (send "Wake" (str "Soft.Order.«" brief " Tag s5a.»") (sender-pane))
            old (str (watch 60 #(awake-flow quaternary)))
            old-pane (bound-pane (inspect quaternary))
            refresh (flow-process (str "Refresh." quaternary) @driver-pane)
            sending (flow-process (str "Send.{ " quaternary " Wake Soft.Order.«" brief " Tag s5b.» }") (sender-pane))
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
         :watched (str "Metaflows straight after: Opus " (name (:state m)) " as " (:flow m) ", Past " (:past m))}))]

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
                       " in its last reading; Opus Awake as " (:flow m))}))]

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
         :watched "three Rebinds of the Opus"}))]

   ["11 topic refusal"
    (fn []
      (let [before (pane-ids)
            reply (flow (str "Launch.{ { Psyche Test Haiku } Predecessors.0 «" brief "» }"))]
        {:reply reply
         :pass (boolean (and (str/includes? reply "TopicRefused") (= before (pane-ids))))
         :watched "no pane opened"}))]

   ["12 Retire: refused while Maintained; retired once declared OnDemand"
    (fn []
      (let [flow-id (str (awake-flow secondary))
            pane (sender-pane)
            refused (flow-meta (str "Retire." flow-id))
            kept (and (contains? (pane-ids) pane) (= (str (awake-flow secondary)) flow-id))
            declared (flow-meta (on-demand-secondary))
            reply (flow-meta (str "Retire." flow-id))
            gone (watch 10 #(not (contains? (pane-ids) pane)))
            m (watch 20 #(let [m (current secondary)] (when (not= (str (:flow m)) flow-id) m)))]
        {:reply (str "Retire while Maintained: " refused " / Configure.Metaflows (Opus OnDemand): "
                     declared " / Retire: " reply)
         :pass (boolean (and (= refused "RetireRejected.Maintained") kept
                             (str/starts-with? declared "Configured.")
                             (str/starts-with? reply "FlowRetired.") gone))
         :watched (str "after the refused Retire, pane " pane (if kept " open and Opus Awake as " " changed; Opus ")
                       flow-id "; after the second, pane " (if gone "closed" "still open") "; Opus afterwards "
                       (if m (str (name (:state m)) " " (:flow m)) "unchanged after 20 s"))}))]])



;; ---------------------------------------------------------------- witnesses

(defn witness-root
  "Where runs keep their witnesses, read from an environment:
  FLOW_CAPSULE_WITNESS_DIR as given, else $HOME/witnesses/<flow id>, the
  flow id FLOW_ID or else the name of the calling flow's FLOW_DIRECTORY.
  Nil without a home or a flow id."
  [env]
  (or (not-empty (get env "FLOW_CAPSULE_WITNESS_DIR"))
      (let [home (not-empty (get env "HOME"))
            flow-id (or (not-empty (get env "FLOW_ID"))
                        (some-> (not-empty (get env "FLOW_DIRECTORY")) fs/file-name not-empty))]
        (when (and home flow-id)
          (str (str/replace home #"/+$" "") "/witnesses/" flow-id)))))

(defn witness-name
  "A run's witness directory: the flow version the Nexus reports, and the
  UTC time the run started."
  [version-line instant]
  (str "capsule-" (last (str/split (str/trim version-line) #"\s+")) "-"
       (.format (java.time.format.DateTimeFormatter/ofPattern "yyyyMMdd'T'HHmmss'Z'")
                (java.time.ZonedDateTime/ofInstant instant java.time.ZoneOffset/UTC))))

(defn keep-witnesses!
  "Copies each witness that exists, a file or a whole tree, under its name
  in dir; the originals stay for teardown to delete. Answers dir."
  [dir witnesses]
  (fs/create-dirs dir)
  (doseq [[name source] witnesses
          :when (and source (fs/exists? source))]
    (if (fs/directory? source)
      (fs/copy-tree source (str dir "/" name))
      (fs/copy source (str dir "/" name))))
  dir)

(defn teardown-witnesses
  "The teardown copy set. Handover preservation already owns every Codex
  session tree before teardown; other modes copy that tree here."
  [handover? log transcripts sessions]
  (cond-> {"nexus.log" log "claude-transcripts" transcripts}
    (not handover?) (assoc "codex-sessions" sessions)))

(def witness-dir (atom nil))
(def run-log (atom nil))

(defn keep-text!
  "Writes text under name in this run's witness directory."
  [name text]
  (when-let [dir @witness-dir]
    (fs/create-dirs dir)
    (spit (str dir "/" name) (or text ""))))

(defn sha256-file [path]
  (let [digest (java.security.MessageDigest/getInstance "SHA-256")]
    (with-open [in (java.io.BufferedInputStream. (java.io.FileInputStream. (str path)))]
      (let [buffer (byte-array 8192)]
        (loop []
          (let [n (.read in buffer)]
            (when (pos? n)
              (.update digest buffer 0 n)
              (recur))))))
    (format "%064x" (java.math.BigInteger. 1 (.digest digest)))))

(defn preserve-h0-file! [source relative]
  (let [destination (str @witness-dir "/" relative)]
    (if (and source (fs/regular-file? source))
      (do (fs/create-dirs (fs/parent destination))
          (fs/copy source destination {:replace-existing true})
          (fs/set-posix-file-permissions destination "rw-------")
          {:source (str source) :destination destination :present true
           :sha256 (sha256-file destination)})
      {:source (str source) :destination destination :present false})))

(defn preserve-h0-jsonl-tree! [source relative]
  (if (fs/directory? source)
    (let [files (vec (distinct (concat (fs/glob source "*.jsonl")
                                       (fs/glob source "**/*.jsonl"))))]
      (if (seq files)
        (mapv #(preserve-h0-file! % (str relative "/" (fs/relativize source %))) files)
        [{:source (str source) :destination (str @witness-dir "/" relative)
          :present false :reason "no JSONL files"}]))
    [{:source (str source) :destination (str @witness-dir "/" relative)
      :present false :reason "directory absent"}]))

(defn inventory-h0-evidence [name]
  (let [path (str @witness-dir "/" name)]
    (if (fs/regular-file? path)
      (do (fs/set-posix-file-permissions path "rw-------")
          {:source path :destination path :present true :sha256 (sha256-file path)})
      {:source path :destination path :present false :reason "evidence absent"})))

(defn preserve-handover-evidence!
  "Keeps only the ruled native session/index and handover log evidence.
  Every absence and preservation error becomes evidence; none escapes to
  prevent the outer teardown."
  [receipt-name evidence-names]
  (let [receipt (atom [])
        keep #(try (swap! receipt conj (preserve-h0-file! %1 %2))
                   (catch Exception e
                     (swap! receipt conj {:source (str %1) :destination (str %2)
                                          :present false :error (.getMessage e)})))]
    (try
      (swap! receipt into (preserve-h0-jsonl-tree! (str codex-home "/sessions") "codex-sessions"))
      (swap! receipt into (preserve-h0-jsonl-tree! (str codex-home "/archived_sessions") "codex-archived-sessions"))
      (let [indices (vec (fs/glob codex-home "session_index*"))]
        (if (seq indices)
          (doseq [index indices] (keep index (str "codex-session-index/" (fs/file-name index))))
          (swap! receipt conj {:source (str codex-home "/session_index*")
                               :destination (str @witness-dir "/codex-session-index")
                               :present false :reason "session index absent"})))
      (keep @run-log "nexus-and-native.log")
      (keep h0-herdr-stdout "herdr-server.stdout.log")
      (keep h0-herdr-stderr "herdr-server.stderr.log")
      (doseq [name evidence-names]
        (swap! receipt conj (inventory-h0-evidence name)))
      (catch Exception e
        (swap! receipt conj {:present false :error (.getMessage e) :stage "preservation"})))
    (try
      (keep-text! receipt-name
                  (json/generate-string {:time (str (java.time.Instant/now)) :files @receipt} {:pretty true}))
      (fs/set-posix-file-permissions (str @witness-dir "/" receipt-name) "rw-------")
      (catch Exception _ nil))
    @receipt))

(defn preserve-h0-diagnostic! []
  (preserve-handover-evidence!
   "handover-H0-preservation.json"
   [h0-wrapper-record-name h0-sampler-record-name
    "handover-H0-model-catalog.json"
    "handover-H0-diagnostic-prelaunch.json"
    "handover-H0-diagnostic-launch.txt"]))

(defn preserve-full-handover! []
  (preserve-handover-evidence!
   "handover-full-preservation.json"
   [h0-wrapper-record-name "handover-H0-model-catalog.json"]))

(def handover-finish-succeeded (atom true))
(def handover-evidence-preserved (atom true))

(defn preserve-teardown-witnesses!
  "One witness owner after every writer stopped: typed handover evidence for
  either handover mode, the legacy copy set for every other capsule."
  []
  (try
    (if handover-mode
      (let [receipt (if h0-diagnostic-mode (preserve-h0-diagnostic!) (preserve-full-handover!))
            receipt-name (if h0-diagnostic-mode "handover-H0-preservation.json"
                             "handover-full-preservation.json")
            success (and (not-any? :error receipt)
                         (fs/regular-file? (str @witness-dir "/" receipt-name)))]
        (reset! handover-evidence-preserved (boolean success))
        (str "handover witnesses " (if success "preserved" "incomplete") " in " @witness-dir))
      (do (keep-witnesses! @witness-dir
                           (teardown-witnesses false @run-log capsule-transcripts
                                               (str codex-home "/sessions")))
          (reset! handover-evidence-preserved true)
          (str "witnesses kept in " @witness-dir)))
    (catch Exception e
      (reset! handover-evidence-preserved false)
      (str "witnesses not kept in " @witness-dir ": " (.getMessage e)))))

(defn finish-h0-diagnostic! []
  (try (stop-h0-sampler!)
       (catch Exception e
         (append-h0-json! h0-sampler-record-name
                          {:time (str (java.time.Instant/now)) :kind "sampler-stop-error"
                           :error (.getMessage e)}))))

(defn run-with-teardown!
  "Runs the selected scenarios, quiesces their diagnostic sampler, then
  tears down exactly once even when either earlier stage throws."
  [body finish! teardown!]
  (let [clean (atom false)]
    (reset! handover-finish-succeeded true)
    (reset! handover-evidence-preserved true)
    (try
      (body)
      (finally
        (try (finish!)
             (catch Exception e
               (reset! handover-finish-succeeded false)
               (keep-text! "handover-finish-error.txt" (.getMessage e))))
        (reset! clean (teardown!))))
    @clean))

;; ---------------------------------------------------------------- readings

(defn waking-entries
  "The entries of the first vector of NativeInputs in text, as a wake types
  a queue: each {:id :sender :priority}, in the order typed. Nil when text
  holds none."
  [text]
  (when-let [m (some->> text (re-matcher #"\[\s*\{\s*logical-delivery-"))]
    (when (.find m)
      (let [d (read-datom (subs text (.start m)))]
        (when (= \[ (:enclosure d))
          (vec (for [entry (items d)
                     :let [[id sender message] (items entry)]]
                 {:id (str id)
                  :sender (if (map? sender)
                            (str/join " " (cons (head sender) (map str (items (body sender)))))
                            (str sender))
                  :priority (when (map? message) (head message))})))))))

(defn admitted-deadline
  "The deadline, in Unix seconds, the Nexus logged when it admitted the Task
  id; nil when it logged none."
  [log id]
  (some->> (re-find (re-pattern (str "flow-nexus: admitted " (java.util.regex.Pattern/quote id)
                                     " Task minutes \\S+ deadline (\\d+) at "))
                    (or log ""))
           second
           parse-long))

(defn reminder-lag
  "Seconds from a logged deadline to an ISO-8601 transcript stamp."
  [deadline stamp]
  (- (.getEpochSecond (java.time.Instant/parse stamp)) deadline))

;; A reminder is due at its deadline and sent at the Nexus's next reminder
;; tick (5 s), then woken or handed over to its sender's seat; 30 s holds
;; both. It is never early.
(def reminder-tolerance-seconds 30)

(defn on-time? [lag] (<= 0 lag reminder-tolerance-seconds))

(defn accepted-id
  "The logical delivery id a SendAccepted reply carries; nil for any other reply."
  [reply]
  (let [d (read-datom reply)]
    (when (= "SendAccepted" (head d))
      (some-> d body items first str not-empty))))

(defn holds-id?
  "Whether text holds the id, written out; never for a missing or blank id."
  [text id]
  (and (not (str/blank? id)) (str/includes? (str text) id)))

(defn queued-ids
  "The logical delivery ids queued at the Address, from a Listed datom."
  [listed addr]
  (mapv #(str (first (items %))) (:queue (metaflow listed addr))))

(defn notice-queued?
  "Whether the Notice id is queued at the Address in a Metaflows (Listed)
  reply: the Notice lives in the metaflow's queue, which ReadQueueDeliveries
  does not list. Never for a blank or missing id."
  [metaflows addr id]
  (boolean (and (not (str/blank? id))
                (some #{id} (queued-ids (read-datom metaflows) addr)))))

(defn delivery-states
  "Each logical delivery's state by id, from an ObserveSends first frame,
  SendsObserved.{ cursor [ { cursor id sent-by address state } ... ] }; the
  state is its head (Delivered.Receipt reads Delivered). Empty for any other
  reply, a refusal among them."
  [frame]
  (let [d (read-datom frame)]
    (if (= "SendsObserved" (head d))
      (into (sorted-map)
            (for [snapshot (items (second (items (body d))))
                  :let [[_ id _ _ state] (items snapshot)]
                  :when (string? id)]
              [id (if (map? state) (or (:head state) (str state)) (str state))]))
      {})))

(defn deliveries-preserved?
  "Whether, after the restart, every logical delivery read before the stop is
  held in the same state or advanced to Delivered (a Prepared or Uncertain
  attempt whose transcript shows the receipt settles at restart), none lost.
  Nothing read before the stop is no witness."
  [before after]
  (boolean (and (seq before)
                (every? (fn [[id state]]
                          (let [now (get after id)]
                            (and now (or (= now state) (= now "Delivered")))))
                        before))))

(defn seat-declaration
  "The FlowKind and wake policy a Configure.Metaflows line declares for the
  Address, e.g. [\"Regular\" \"OnDemand\"]."
  [metaflows-line addr]
  (let [want (map str (address-items addr))]
    (some (fn [entry]
            (let [[a _root _ _ kind _ wake] (items entry)]
              (when (= (map str (items a)) want)
                [(str kind) (str wake)])))
          (items (loop [d (read-datom metaflows-line)] (if (head d) (recur (body d)) d))))))

;; ---------------------------------------------------------------- capsule life

(def nexus (atom nil))
;; The takeover's Nexus processes, old and new, every one it started; and
;; its sender loop, standing in for the trackers' cron.
(def takeover-nexuses (atom []))
(def sender (atom nil))
(def poller (atom nil))
(def natives-seen (atom #{}))
(def herdr-server (atom nil))

(defn capsule-natives
  "The native sessions of the capsule's seats: each transcript in the capsule
  project's directory, and each session a reply named."
  []
  (into (set (map #(str (fs/strip-ext (fs/file-name %)))
                  (when (fs/exists? capsule-transcripts) (fs/glob capsule-transcripts "*.jsonl"))))
        (mapcat #(re-seq #"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}" (str (:reply %))) @results)))

(defn traces
  "Every path that carries one of the capsule's flows: an entry of the default
  flows directory named by a capsule flow, the capsule's flows directory, and
  each Claude entry, two levels under ~/.claude, named by a capsule session."
  [natives]
  (let [flows @seen-flows
        claude-entries (when (fs/exists? claude-root)
                         (let [top (vec (fs/list-dir claude-root))]
                           (concat top (mapcat #(when (fs/directory? %) (fs/list-dir %)) top))))]
    (vec (concat
          (for [entry (when (fs/exists? default-flows) (fs/list-dir default-flows))
                :let [n (str (fs/file-name entry))]
                :when (some #(str/includes? n %) flows)]
            (str entry))
          (when (fs/exists? capsule-flows) [capsule-flows])
          (when (fs/exists? capsule-transcripts) [capsule-transcripts])
          (for [entry claude-entries
                :let [n (str (fs/file-name entry))]
                :when (some #(str/includes? n %) natives)]
            (str entry))))))

(defn history-lines
  "The lines of Claude's shared prompt history that name a capsule session;
  the file is shared with every running Claude session and is not written."
  [natives]
  (let [file (str claude-root "/history.jsonl")]
    (if (and (seq natives) (fs/exists? file))
      (count (filter (fn [line] (some #(str/includes? line %) natives)) (str/split-lines (slurp file))))
      0)))

(defn project-entry?
  "Whether Claude's shared configuration holds an entry for the capsule's
  source root; the file is shared with every running Claude and is not written."
  []
  (let [file (str home "/.claude.json")]
    (boolean (and (fs/exists? file)
                  (contains? (get (try (json/parse-string (slurp file)) (catch Exception _ {})) "projects" {})
                             source-root)))))

(defn refuse [why] (binding [*out* *err*] (println "flow-capsule:" why)) (System/exit 2))

(defn prepare-private-home-and-flow-wrapper! []
  (let [bin (str private-root "/bin")
        config (str state-root "/flow-client-socket")
        wrapper (str bin "/flow")
        zshrc (str home "/.zshrc")]
    (fs/create-dirs bin)
    (fs/create-dirs home)
    ;; Herdr opens an interactive shell before it starts a native client. A
    ;; fresh private HOME therefore carries the smallest ordinary zsh setup,
    ;; so zsh starts that client instead of opening its first-run questionnaire.
    (spit zshrc "# Flow capsule private shell.\n")
    (spit config (str ordinary-socket "\n"))
    (spit wrapper
      (str "#!/bin/sh\nset -eu\n[ \"$#\" -eq 1 ] || exit 64\n"
           "socket=$(cat '" config "')\n"
           "invocation=$(" @package "/bin/flow-compose-invocation \"$socket\" \"$1\")\n"
           "exec " @package "/bin/flow \"$invocation\"\n"))
    (fs/set-posix-file-permissions zshrc "rw-------")
    (fs/set-posix-file-permissions wrapper "rwx------")
    (fs/set-posix-file-permissions config "rw-------")))

(defn verify-private-handover-places! []
  (when handover-mode
    (when-not (= codex-model "gpt-5.6-luna")
      (throw (ex-info "native declaration requires parent disposition before substitution" {:model codex-model})))
    (doseq [path [home capsule-runtime state-root store-path config-home codex-home codex-socket codex-client
                 curriculum-socket curriculum-contexts source-parent source-root capsule-flows workspace-a workspace-b]]
      (when-not (.startsWith (.normalize (fs/path path)) (fs/path private-root))
        (throw (ex-info "capsule path escaped private root" {:path path}))))
    (when-not (and (str/starts-with? (str @witness-dir) "/tmp/")
                   (not (str/starts-with? (str @witness-dir) (str private-root "/"))))
      (throw (ex-info "capsule witnesses require separate private temporary storage" {:path @witness-dir})))
    (when (fs/exists? source-parent)
      (throw (ex-info "capsule root must be fresh; preserve existing contents" {:path source-parent})))
    (keep-text! "handover-private-paths.txt"
      (pr-str {:root private-root :home home :runtime capsule-runtime :memory store-path
               :herdr-config config-home :session session :source source-root
               :codex {:binary codex-binary :home codex-home :socket codex-socket :client codex-client :model codex-model}
               :unused-next {:model "gpt-6-luna" :launched false}
               :stable-configuration "unchanged"}))))

(defn start-capsule! [log]
  (when (fs/exists? herdr-session-dir) (refuse (str "Herdr session " session " already exists; nothing started")))
  (when (fs/exists? capsule-runtime) (refuse (str capsule-runtime " already exists; nothing started")))
  (when (fs/exists? source-parent) (refuse (str source-parent " already exists; nothing started")))
  ;; The absence guards above establish ownership before the first mkdir can
  ;; partially succeed. They are milestones, not path guesses.
  (swap! capsule-ownership conj :state-root :source-parent :runtime)
  (fs/delete-tree state-root)
  (fs/create-dirs state-root)
  (fs/create-dirs (str config-home "/herdr"))
  ;; Herdr starts a Codex agent only from an executable its config allows.
  (spit (str config-home "/herdr/config.toml")
        (str "[agents]\ncodex_executables = [\"" codex-client "\"]\n"))
  (fs/create-dirs capsule-runtime)
  (fs/create-dirs capsule-flows)
  (fs/create-dirs workspace-a)
  (fs/create-dirs workspace-b)
  (when handover-mode
    (fs/set-posix-file-permissions private-root "rwx------")
    (prepare-private-home-and-flow-wrapper!))
  (reset! herdr-server
          (p/process ["herdr" "--session" session "server"]
                     (if handover-mode
                       {:env (clean-env) :out :append :out-file (fs/file h0-herdr-stdout)
                        :err :append :err-file (fs/file h0-herdr-stderr) :in ""}
                       {:env (clean-env) :out :discard :err :discard :in ""})))
  (swap! capsule-ownership conj :herdr-session)
  (when-not (watch 15 #(get-in (herdr-json "workspace" "list") [:result :workspaces]))
    (throw (ex-info "Herdr session did not answer" {})))
  (let [created (herdr-json "workspace" "create" "--cwd" source-root "--label" "capsule-driver" "--no-focus")]
    (reset! driver-pane (get-in created [:result :root_pane :pane_id]))))

;; ---------------------------------------------------------------- curriculum

(def curriculum (atom nil))
(def curriculum-environment (atom nil))

(def context-roots
  "Each context repository as Curriculum's environment names its directory
  and origin."
  [["CURRICULUM_PSYCHES_REPOSITORY_DIR" "CURRICULUM_PSYCHES_REPOSITORY_URL" "psyche-belief"]
   ["CURRICULUM_MIND_CONTEXT_DIR" "CURRICULUM_MIND_CONTEXT_URL" "mind-belief"]
   ["CURRICULUM_FIELD_CONTEXT_DIR" "CURRICULUM_FIELD_CONTEXT_URL" "field-belief"]])

(defn stable-curriculum-environment
  "The running Curriculum's configuration: its unit's environment, read
  through systemd and never written."
  []
  (let [{:keys [exit out err]} (run {:env (merge (clean-env) {"XDG_RUNTIME_DIR" runtime-root})}
                                    "systemctl" "--user" "show" curriculum-unit "-p" "Environment" "--value")]
    (when-not (zero? exit) (throw (ex-info "the running Curriculum's unit environment is unreadable" {:err err})))
    (into {} (keep #(let [[k v] (str/split % #"=" 2)] (when v [k v])) (str/split (str/trim out) #"\s+")))))

(defn start-curriculum! [log]
  (let [stable (stable-curriculum-environment)
        layout (get stable "CURRICULUM_CONTEXT_LAYOUT")
        no-refresh (str (fs/which "true"))
        env (into (clean-env {"CURRICULUM_CONTEXT_LAYOUT" layout
                              "CURRICULUM_CONTEXT_REFRESH_PROGRAM" no-refresh
                              "CURRICULUM_RECORDS_DIR" capsule-flows})
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
        (println (str "curriculum context " name " | " url " main " head))))
    (reset! curriculum-environment env)
    ;; The Nexus serves only in a runtime directory that exists, as its unit's
    ;; RuntimeDirectory=curriculum (mode 0700) makes it; it opens none itself.
    (fs/create-dirs (fs/parent curriculum-socket))
    (fs/set-posix-file-permissions (fs/parent curriculum-socket) "rwx------")
    (reset! curriculum (p/process [(str curriculum-package "/bin/curriculum-nexus")]
                                  {:env env :dir source-root :out :append :out-file (fs/file log)
                                   :err :append :err-file (fs/file log) :in ""}))
    (when-not (watch 30 #(or (fs/exists? curriculum-socket) (not (p/alive? @curriculum))))
      (throw (ex-info "the Curriculum Nexus opened no socket" {})))
    (when-not (p/alive? @curriculum)
      (throw (ex-info "the Curriculum Nexus exited" {:log (str/trim (slurp log))})))
    (let [probe (reply-text (run {:env env} (str curriculum-package "/bin/curriculum") "ResolveSkills.[ spirit:core intent:models ]"))]
      (println (str "curriculum | " curriculum-package " | layout " layout ", refresh " no-refresh
                    ", records " capsule-flows " | socket " curriculum-socket " | ResolveSkills -> " probe)))))

(defn fill
  "A configure.datom line with the capsule's places filled in."
  [line]
  (-> line (str/replace "@SOURCE_ROOT@" source-root) (str/replace "@CAPSULE_RUNTIME@" capsule-runtime)
      (str/replace "@CURRICULUM_SOCKET@" curriculum-socket) (str/replace "@CODEX_CLIENT@" codex-client)
      (str/replace "@CODEX_HOME@" codex-home) (str/replace "@CODEX_SOCKET@" codex-socket)
      (str/replace "@CODEX_MODEL@" codex-model)))

(def codex-server (atom nil))

(defn prepare-h0-exec-wrapper-telemetry! []
  (let [telemetry (str private-root "/bin/codex-exec-telemetry.py")
        record (str @witness-dir "/" h0-wrapper-record-name)]
    (fs/create-dirs @witness-dir)
    (spit telemetry
          (str "import datetime, json, os, pathlib, sys\n"
               "record, argv = sys.argv[1], sys.argv[2:]\n"
               "allowed = ['HOME','CODEX_HOME','PATH','TERM','LANG','SHELL','XDG_RUNTIME_DIR','XDG_CONFIG_HOME','HERDR_SESSION','HERDR_PANE_ID']\n"
               "entry = {'time': datetime.datetime.now(datetime.timezone.utc).isoformat(), "
               "'pid': int(os.environ['FLOW_CAPSULE_EXEC_PID']), 'cwd': os.getcwd(), "
               "'argv': argv, 'environment': {k: os.environ[k] for k in allowed if k in os.environ}}\n"
               "pathlib.Path(record).parent.mkdir(parents=True, exist_ok=True)\n"
               "with open(record, 'a', encoding='utf-8') as out: out.write(json.dumps(entry, ensure_ascii=False) + '\\n')\n"
               "os.chmod(record, 0o600)\n"))
    (fs/set-posix-file-permissions telemetry "rw-------")
    {:script telemetry :record record}))

(defn codex-client-script [diagnostic telemetry record]
  (str "#!/bin/sh\nexport CODEX_HOME=" codex-home "\n"
       (when diagnostic
         (str "FLOW_CAPSULE_EXEC_PID=$$ python3 '" telemetry "' '" record "' '"
              codex-binary "' \"$@\"\n"))
       "exec " codex-binary " \"$@\"\n"))

(defn start-codex! [log]
  (when-not codex-binary (throw (ex-info "no Codex executable: set FLOW_CAPSULE_CODEX" {})))
  (fs/create-dirs codex-home)
  (fs/create-dirs (fs/parent codex-client))
  (fs/create-dirs (fs/parent codex-socket))
  (fs/copy codex-login (str codex-home "/auth.json") {:replace-existing true})
  (fs/set-posix-file-permissions (str codex-home "/auth.json") "rw-------")
  (spit (str codex-home "/config.toml")
        (str "approval_policy = \"never\"\nsandbox_mode = \"danger-full-access\"\nmodel = \"" codex-model "\"\n"))
  (let [{:keys [script record]} (when handover-mode (prepare-h0-exec-wrapper-telemetry!))]
    (spit codex-client (codex-client-script handover-mode script record)))
  (fs/set-posix-file-permissions codex-client "rwx------")
  (reset! codex-server (p/process [codex-binary "app-server" "--remote-control" "--listen" (str "unix://" codex-socket)]
                                  {:env (clean-env {"CODEX_HOME" codex-home}) :dir source-root
                                   :out :append :out-file (fs/file log) :err :append :err-file (fs/file log) :in ""}))
  (when-not (watch 30 #(fs/exists? codex-socket))
    (throw (ex-info "the capsule's Codex app-server opened no socket" {})))
  (println (str "codex | " codex-binary " | " codex-model " | " codex-socket)))

(defn start-text
  ([] (start-text ordinary-socket meta-socket store-path))
  ([ordinary meta store] (str "{ «" ordinary "» «" meta "» «" store "» None }")))

(defn start-nexus! [log]
  (reset! nexus (p/process [(str @package "/bin/flow-nexus") (start-text)]
                           {:env (clean-env) :out :append :out-file (fs/file log) :err :append :err-file (fs/file log) :in ""}))
  (when-not (watch 20 #(and (fs/exists? ordinary-socket) (fs/exists? meta-socket)))
    (throw (ex-info "the Nexus opened no sockets" {:log (str/trim (slurp log))}))))

(defn listens?
  "Whether this exact process listens on the meta socket, the capsule's or
  the one given."
  ([pid] (listens? pid meta-socket))
  ([pid meta-socket]
  (let [inodes (set (keep #(some->> (try (str (fs/read-link %)) (catch Exception _ nil)) (re-find #"socket:\[(\d+)\]") second)
                          (try (fs/list-dir (str "/proc/" pid "/fd")) (catch Exception _ []))))]
    (some (fn [row]
            (let [cols (str/split (str/trim row) #"\s+")]
              (and (= (get cols 7) meta-socket) (= (get cols 3) "00010000") (inodes (get cols 6)))))
          (rest (str/split-lines (try (String. (java.nio.file.Files/readAllBytes (fs/path (str "/proc/" pid "/net/unix")))) (catch Exception _ ""))))))))

(defn configure-one!
  "One Configure through a configuration-only Nexus, which serves one request
  and exits: of the package in charge on the capsule's sockets and store, or
  of pkg on the sockets and store start names. Its caller carries no pane
  marks: such a peer is refused there."
  ([datom log] (configure-one! datom log @package (start-text) meta-socket))
  ([datom log pkg start meta-socket]
    (let [child (p/process [(str pkg "/bin/flow-nexus") "--configure-only" start]
                           {:env (clean-env {"PATH" (str pkg "/bin:" (System/getenv "PATH"))})
                            :out :append :out-file (fs/file log) :err :append :err-file (fs/file log) :in ""})
          pid (.pid (:proc child))]
      (when-not (watch 10 #(or (listens? pid meta-socket) (not (p/alive? child))))
        (p/destroy child))
      (let [reply (if (p/alive? child)
                    (reply-text (run {:env (clean-env {"PATH" (str pkg "/bin:" (System/getenv "PATH"))})}
                                     (str pkg "/bin/flow-meta")
                                     (client-argument pkg "flow-meta" meta-socket datom)))
                    (str "configuration-only Nexus exited before listening: " (str/trim (slurp log))))
            exit (:exit (deref child 15000 {:exit "still running"}))]
        (when (= exit "still running") (p/destroy child))
        [reply exit]))))

(defn configure-lines
  "The configure.datom lines that are datoms, their placeholders filled."
  []
  (vec (for [line (str/split-lines (slurp (fs/file libexec (if handover-mode "configure-handover.datom" "configure.datom"))))
             :when (and (not (str/blank? line)) (not (str/starts-with? line ";")))]
         (fill line))))

(defn configure!
  "Each datom through a configuration-only Nexus of the package in charge:
  configure.datom's, or the lines given."
  [log & [lines]]
  (doseq [datom (or lines (configure-lines))]
    (let [[reply exit] (configure-one! datom log)
          family (second (re-find #"^Configure\.([A-Za-z]+)" datom))
          ok (str/starts-with? reply "Configured.")]
      (println (str "configure " family " | " reply " | " (if ok "pass" "fail") " | configuration-only Nexus exit " exit))
      (flush)
      (when-not ok (throw (ex-info (str "Configure." family " refused") {:reply reply}))))))

(def handover-h0-refusals (atom nil))

(defn require-handover-identifier
  "A handover identifier established by H0, or a bounded failure that keeps
  H0's exact launch refusals. No dependent client receives an empty id."
  [kind value & [address]]
  (let [identifier (some-> value str not-empty)]
    (when (str/blank? identifier)
      (throw (ex-info "handover prerequisite identifier missing after H0"
                      (cond-> {:prerequisite "H0" :identifier kind
                               :h0-refusals @handover-h0-refusals}
                        address (assoc :address address)))))
    identifier))

(defn read-events-query [flow-id]
  (str "ReadEvents." (require-handover-identifier :flow flow-id)))

(defn handover-flow [address]
  (require-handover-identifier :flow (awake-flow address) address))

(defn handover-thread [address]
  (require-handover-identifier :native-thread (native-session (inspect address)) address))

(defn turns
  "The Haiku turns the capsule's flows took: the prompts each flow's harness
  reported submitted."
  []
  (into {} (for [f @seen-flows]
             (let [reply (flow-meta (read-events-query f))]
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
                    (str/includes? env (str "XDG_RUNTIME_DIR=" capsule-runtime "\u0000"))
                    (str/includes? cmd (str "--session " session))
                    (str/includes? env (str "HERDR_SESSION=" session)))]
      (str pid " " (str/trim cmd)))))

(def herdr-session-left (atom nil))

(defn teardown! []
  (println "teardown")
  (let [started (System/currentTimeMillis)
        steps (atom [])
        cleanup (cleanup-plan @capsule-ownership)]
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
    (when (:herdr-session cleanup)
      (doseq [w (get-in (herdr-json "workspace" "list") [:result :workspaces])]
        (swap! steps conj (str "close workspace " (:workspace_id w) " (" (:label w) ") -> " (str/trim (:out (herdr "workspace" "close" (:workspace_id w))))))))
    (when-let [{:keys [running future]} @sender]
      (reset! running false)
      (deref future 30000 nil)
      (swap! steps conj "takeover sender loop stopped"))
    (when-let [{:keys [running future]} @poller]
      (reset! running false)
      (deref future 5000 nil))
    (doseq [n @takeover-nexuses :when (p/alive? n)]
      (p/destroy n)
      (swap! steps conj (str "takeover Nexus " (.pid (:proc n)) " stopped, exit "
                             (:exit (deref n 20000 {:exit "still running"})))))
    (when-let [n @nexus]
      (p/destroy n)
      (swap! steps conj (str "Nexus stopped, exit " (:exit (deref n 20000 {:exit "still running"})))))
    (when-let [c @codex-server]
      (p/destroy-tree c)
      (swap! steps conj (str "Codex app-server stopped, exit " (:exit (deref c 20000 {:exit "still running"})))))
    (when-let [c @curriculum]
      (p/destroy c)
      (swap! steps conj (str "Curriculum Nexus stopped, exit " (:exit (deref c 20000 {:exit "still running"})))))
    (when-let [h @herdr-server]
      (herdr "server" "stop")
      (when (= ::running (deref h 10000 ::running)) (p/destroy-tree h))
      (swap! steps conj (str "Herdr session " session " stopped")))
    (when (:herdr-session cleanup)
      (run {} "herdr" "session" "delete" session))
    ;; Every process that writes them has stopped: the Nexus log and each
    ;; flow transcript are kept before the state root and traces go.
    (when @witness-dir
      (swap! steps conj (preserve-teardown-witnesses!)))
    ;; The capsule's Herdr keeps its sessions in the capsule's own config
    ;; home: its session is gone when its directory is, read before that
    ;; home is deleted with the state root, and no Herdr runs after it.
    (reset! herdr-session-left (and (:herdr-session cleanup) (fs/exists? herdr-session-dir)))
    (when (:traces cleanup)
      (let [natives (reset! natives-seen (capsule-natives))
            found (traces natives)]
        (doseq [t found] (fs/delete-tree t))
        (swap! steps conj (str "capsule traces removed " (count found) " for " (count @seen-flows) " flows and "
                               (count natives) " Claude sessions"))))
    (when (:state-root cleanup) (fs/delete-tree state-root))
    (when (:source-parent cleanup) (fs/delete-tree source-parent))
    (when (:runtime cleanup) (fs/delete-tree capsule-runtime))
    (doseq [s @steps] (println (str "  " s)))
    (let [remaining {:herdr-session @herdr-session-left
                     :handover-finish @handover-finish-succeeded
                     :handover-evidence @handover-evidence-preserved
                     :processes (vec (or (watch 20 #(let [left (capsule-processes)] (when (empty? left) []))) (capsule-processes)))
                     :store (fs/exists? state-root)
                     :source-root (fs/exists? source-parent)
                     :sockets (fs/exists? capsule-runtime)
                     :curriculum-process (boolean (some-> @curriculum p/alive?))
                     :curriculum-store (fs/exists? curriculum-contexts)
                     :curriculum-socket (fs/exists? curriculum-socket)
                     :traces (traces @natives-seen)
                     :history-lines (history-lines @natives-seen)
                     :claude-project-entry (project-entry?)}
          clean (and (:handover-finish remaining) (:handover-evidence remaining)
                     (not (:herdr-session remaining)) (empty? (:processes remaining)) (not (:store remaining))
                     (not (:sockets remaining)) (not (:source-root remaining)) (empty? (:traces remaining))
                     (not (:curriculum-process remaining)) (not (:curriculum-store remaining))
                     (not (:curriculum-socket remaining)))]
      (print-line {:name "13 teardown" :reply (pr-str remaining) :pass clean
                   :ms (- (System/currentTimeMillis) started)
                   :watched (str "Herdr session list; /proc cmdline and environ, waiting up to 20 s for capsule processes to end; store, source root, sockets; the Curriculum Nexus process, its context checkouts and socket; traces of the capsule's flows in " default-flows " and " claude-root "; history.jsonl lines naming a capsule session and ~/.claude.json's entry for the source root are reported and left")})
      clean)))

;; ---------------------------------------------------------------- night scenarios
;; A refused successor keeps its flow, is told and is handed its queue; a
;; Task's Minutes is ruled at admission, its reminder is held across a
;; restart, and it is reminded until its Done.

(def nexus-log (atom nil))

(defn configure-line
  "The configure.datom line of a family, its placeholders filled."
  [family]
  (fill (first (filter #(str/starts-with? % (str "Configure." family "."))
                       (str/split-lines (slurp (fs/file libexec (if handover-mode "configure-handover.datom" "configure.datom"))))))))

(def missing-module "vision:no-such-module-2a9116")

(defn broken-metaflows
  "The capsule's Metaflows with the seat at addr declaring a module no
  context repository holds, so its successor cannot compose."
  [addr]
  (let [line (configure-line "Metaflows")
        [_ aspect topic model] (re-find #"\{ (\w+) (\w+) (\w+) \}" addr)
        seat (re-pattern (str "\\{ \\{ " aspect " " topic " " model " \\} \\[ spirit:core intent:models \\]"))]
    (str/replace-first line seat (str "{ { " aspect " " topic " " model " } [ spirit:core " missing-module " ]"))))

(defn restore-configuration! []
  [(flow-meta (configure-line "Metaflows")) (flow-meta (configure-line "Thresholds"))])

(defn stops-of [flow-id]
  (count (heads-named "Stopped" (read-datom (flow-meta (read-events-query flow-id))))))

(defn log-text [] (try (slurp @nexus-log) (catch Exception _ "")))

(defn transcript-holds
  "Whether the transcript of the metaflow's current Claude session holds
  needle within seconds: a pane wraps long lines, a transcript does not."
  [addr needle seconds]
  (watch seconds #(when-let [native (native-session (inspect addr))]
                    (when-let [file (first (fs/glob (str home "/.claude/projects") (str "*/" native ".jsonl")))]
                      (str/includes? (slurp (str file)) needle)))))

(defn reminder-stamps
  "The transcript stamps of every line of the metaflow's current Claude
  session that holds needle."
  [addr needle]
  (let [native (native-session (inspect addr))
        file (when native (first (fs/glob (str home "/.claude/projects") (str "*/" native ".jsonl"))))]
    (keep #(when (str/includes? % needle) (second (re-find #"\"timestamp\":\"([^\"]+)\"" %)))
          (when file (str/split-lines (slurp (str file)))))))

(defn pane-holds [pane needle seconds]
  (watch seconds #(some-> (pane-text pane) (str/includes? needle))))

;; The seat scenario 20 wakes: a Regular OnDemand seat, which takes its whole
;; queue on a wake (a Simplified seat takes its oldest alone).
(def waking-seat tertiary-address)

(def night-scenarios
  [["13 OnDemand seat whose successor cannot compose is kept and handed its queue, and no flow is told"
    (fn []
      (let [_ (flow (str "Launch.{ " tertiary-address " Predecessors.0 «Run the shell command sleep 30, then answer with one word and stop. Tag ra1.» }"))
            flow-id (str (watch 90 #(awake-flow tertiary-address)))
            pane (bound-pane (inspect tertiary-address))
            broken (flow-meta (broken-metaflows tertiary-address))
            rested (watch 150 #(pos? (stops-of flow-id)))
            _ (Thread/sleep 8000)
            kept (= (str (awake-flow tertiary-address)) flow-id)
            open (contains? (pane-ids) pane)
            kept-line (some #(when (and (str/includes? % flow-id) (str/includes? % "kept at its Stop")) %) (str/split-lines (log-text)))
            untold (not (pane-holds (sender-pane) (str "(flow " flow-id ") was refused") 60))
            queued (send-to tertiary-address "Queue" "Soft.Notice.«Answer with one word and stop. Tag ra2.»" (sender-pane))
            handed (pane-holds pane "Tag ra2" 90)
            restored (restore-configuration!)]
        {:reply (str "Configure broken: " broken " / Send Tag ra2: " queued " / restore: " restored)
         :pass (boolean (and (str/starts-with? broken "Configured.") rested kept open kept-line untold handed))
         :watched (str "Sonnet flow " flow-id " rested " rested "; still Awake " kept ", pane " pane (if open " open" " closed")
                       "; log: " kept-line "; the sender's pane shows no refusal " untold
                       "; Tag ra2 typed into the kept seat " (boolean handed))}))]

   ["14 Maintained seat whose successor cannot compose is kept and handed its queue"
    (fn []
      (let [flow-id (str (awake-flow secondary))
            pane (sender-pane)
            before (stops-of flow-id)
            broken (flow-meta (broken-metaflows secondary))
            low (flow-meta "Configure.Thresholds.[ { claude-haiku-5-5 1000 } ]")
            sent (flow (str "Send.{ " secondary " Wake Soft.Notice.«Answer with one word and stop. Tag rb1.» }") pane)
            rested (watch 120 #(> (stops-of flow-id) before))
            _ (Thread/sleep 8000)
            kept (= (str (awake-flow secondary)) flow-id)
            kept-line (some #(when (and (str/includes? % flow-id) (str/includes? % "RefreshLaunchRefused")) %) (str/split-lines (log-text)))
            queued (flow (str "Send.{ " secondary " Queue Soft.Notice.«Answer with one word and stop. Tag rb2.» }") pane)
            handed (pane-holds pane "Tag rb2" 90)
            restored (restore-configuration!)]
        {:reply (str "Send Tag rb1: " sent " / Send Tag rb2: " queued " / restore: " restored)
         :pass (boolean (and (str/starts-with? broken "Configured.") rested kept kept-line handed))
         :watched (str "flow " flow-id " rested " rested "; still Awake " kept "; log: " kept-line
                       "; Tag rb2 typed into the kept seat " (boolean handed))}))]

   ["15 Task: Minutes zero and below refused at admission, never queued"
    (fn []
      (let [pane (sender-pane)
            before (count (:queue (current quaternary)))
            zero (send-to quaternary "Wake" "Soft.Task.{ 0 «Answer with one word and stop. Tag rc1.» }" pane)
            negative (send-to quaternary "Queue" "Soft.Task.{ -1 «Answer with one word and stop. Tag rc1.» }" pane)
            unreadable (send-to quaternary "Queue" "Soft.Task.{ soon «Answer with one word and stop. Tag rc1.» }" pane)
            after (count (:queue (current quaternary)))]
        {:reply (str "zero: " zero " / negative: " negative " / unreadable: " unreadable)
         :pass (boolean (and (= zero "Refused.MinutesRefused.Zero")
                             (= negative "Refused.MinutesRefused.Negative")
                             (str/starts-with? unreadable "exit 2")
                             (= before after)))
         :watched (str "Psyche test Haiku's queue held " before " before and " after
                       " after; the unreadable Minutes never left the CLI")}))]

   ["16 Task reminder held across a Nexus restart; the Task leaves no queued entry"
    (fn []
      (let [pane (sender-pane)
            sent (send-to quaternary "Queue" "Soft.Task.{ 1 «Answer with one word and stop. Tag rd1.» }" pane)
            id (accepted-id sent)
            n @nexus
            _ (p/destroy n)
            stopped (:exit (deref n 20000 {:exit "still running"}))
            _ (fs/delete-if-exists ordinary-socket)
            _ (fs/delete-if-exists meta-socket)
            _ (start-nexus! @nexus-log)
            reminded (transcript-holds secondary (str "Reminder 1 of 4: task " id) 150)
            done (flow (str "Send.{ " secondary " Queue Soft.Done.{ " id " «closed» } }") pane)
            ;; The Task was queued at an asleep seat that nothing wakes here:
            ;; its entry is withdrawn, so no later scenario finds it queued.
            queued (some #{id} (queued-ids (:datom (metaflows)) quaternary))
            withdrawn (when queued (flow-meta (str "Withdraw.{ " quaternary " «" id "» }")))
            left (queued-ids (:datom (metaflows)) quaternary)]
        {:reply (str "Task: " sent " / Nexus stopped, exit " stopped " / restarted / Done: " done
                     " / Withdraw: " (or withdrawn "not queued"))
         :pass (boolean (and id (str/starts-with? sent "SendAccepted.") reminded
                             (str/starts-with? done "SendAccepted.")
                             (or (not queued) (str/starts-with? withdrawn "Withdrawn."))
                             (not (some #{id} left))))
         :watched (str "Opus transcript shows Reminder 1 of 4 for " id " after the restart "
                       (boolean reminded) "; the Done closes it; the Task " (if queued "withdrawn" "not queued")
                       ", Haiku queue after " left)}))]

   ["17 Task reminded at its minutes; Done closes it; Done again refused"
    (fn []
      (let [pane (sender-pane)
            sent (flow (str "Send.{ " quaternary " Wake Soft.Task.{ 1 «Answer with one word and stop. Tag re1.» } }") pane)
            id (some-> (read-datom sent) body items first str)
            reminded (transcript-holds secondary (str "Reminder 1 of 4: task " id) 150)
            done (flow (str "Send.{ " secondary " Queue Soft.Done.{ " id " «closed» } }") pane)
            again (flow (str "Send.{ " secondary " Queue Soft.Done.{ " id " «again» } }") pane)
            deadline (when id (admitted-deadline (log-text) id))
            stamp (when id (first (reminder-stamps secondary (str "Reminder 1 of 4: task " id))))
            lag (when (and deadline stamp) (reminder-lag deadline stamp))
            _ (Thread/sleep 75000)
            second-reminder (transcript-holds secondary (str "Reminder 2 of 4: task " id) 1)]
        {:reply (str "Task: " sent " / Done: " done " / Done again: " again)
         :pass (boolean (and id reminded (str/starts-with? done "SendAccepted.")
                             (str/includes? again "NoOpenTask") (not second-reminder)
                             lag (on-time? lag)))
         :watched (str "Opus transcript shows Reminder 1 of 4 for " id " " (boolean reminded)
                       "; Nexus log deadline " deadline ", Reminder 1 stamped " stamp ", " lag
                       " s after it (on time: 0 to " reminder-tolerance-seconds " s)"
                       "; no Reminder 2 in the 75 s after Done " (not second-reminder))}))]

   ["20 two requests queued to an asleep Regular seat reach it on its wake as one vector, oldest first"
    (fn []
      (let [asleep (watch 150 #(let [m (current waking-seat)]
                                 (when (and (= :asleep (:state m)) (empty? (:queue m))) m)))
            pane (sender-pane)
            first-sent (send-to waking-seat "Queue" "Soft.Notice.«First of two. Tag w1.»" pane)
            second-sent (send-to waking-seat "Wake" (str "Soft.Order.«Second of two. " brief " Tag w2.»") pane)
            ids (vec (keep accepted-id [first-sent second-sent]))
            woke (watch 60 #(awake-flow waking-seat))
            turn (watch 60 #(transcript-first-user (native-session (inspect waking-seat))))
            entries (waking-entries turn)
            woken-pane (bound-pane (inspect waking-seat))
            shown (watch 60 #(let [t (some-> woken-pane pane-text)]
                               (when (and t (every? (fn [id] (str/includes? t id)) ids)) t)))
            _ (keep-text! "scenario-20-pane.txt" (or shown (some-> woken-pane pane-text)))
            ordered (boolean (and shown (= 2 (count ids))
                                  (< (str/index-of shown (first ids)) (str/index-of shown (second ids)))))]
        {:reply (str "Queue: " first-sent " / Wake: " second-sent)
         :pass (boolean (and asleep woke (= 2 (count ids)) (= ids (mapv :id entries))
                             (every? #(and (:sender %) (:priority %)) entries) ordered))
         :watched (str waking-seat " Asleep with an empty queue before " (boolean asleep) ", woken as " woke
                       "; first turn's vector " (pr-str entries) "; pane " woken-pane
                       " shows both ids oldest first " ordered
                       ", its text kept as scenario-20-pane.txt")}))]])

(def codex-quaternary "{ Mind test Terra }")

(defn codex-sessions-hold
  "Whether any Codex session the capsule's app-server wrote holds needle."
  [needle]
  (some #(str/includes? (slurp (str %)) needle)
        (when (fs/exists? (str codex-home "/sessions")) (fs/glob (str codex-home "/sessions") "**.jsonl"))))

(def codex-scenarios
  [["18 Codex seat: launched on the capsule's own app-server, handed a queued order at rest"
    (fn []
      (let [reply (launch codex-quaternary)
            awake (when (str/starts-with? reply "Started.") (watch 60 #(awake-flow codex-quaternary)))
            queued (send-to codex-quaternary "Queue" "Soft.Order.«Answer with one word and stop. Tag c1.»" (sender-pane))
            handed (watch 120 #(codex-sessions-hold "Tag c1"))]
        {:reply (str "Launch: " reply " / Send: " queued)
         :pass (boolean (and awake (str/starts-with? queued "SendAccepted.") handed))
         :watched (str "Mind test Terra Awake as " awake "; a session under " codex-home
                       (if handed " holds" " lacks") " Tag c1 within 120 s")}))]

   ["19 Codex and Claude exchange one Task and one Done; one reminder at the deadline"
    (fn []
      (let [codex (or (awake-flow codex-quaternary)
                      (do (launch codex-quaternary) (watch 60 #(awake-flow codex-quaternary))))
            codex-pane (bound-pane (inspect codex-quaternary))
            admitted (java.time.Instant/now)
            sent (send-to codex-quaternary "Wake" "Soft.Task.{ 1 «Answer with one word and stop. Tag g1.» }" (sender-pane))
            id (some-> (read-datom sent) body items first str)
            handed (watch 120 #(codex-sessions-hold "Tag g1"))
            reminded (transcript-holds secondary (str "Reminder 1 of 4: task " id) 150)
            done (send-to secondary "Queue" (str "Soft.Done.{ " id " «finished» }") codex-pane)
            _ (Thread/sleep 75000)
            second-reminder (transcript-holds secondary (str "Reminder 2 of 4: task " id) 1)
            native (native-session (inspect secondary))
            file (when native (first (fs/glob (str home "/.claude/projects") (str "*/" native ".jsonl"))))
            lines (filter #(str/includes? % (str "Reminder 1 of 4: task " id))
                          (when file (str/split-lines (slurp (str file)))))
            stamps (keep #(second (re-find #"\"timestamp\":\"([^\"]+)\"" %)) lines)
            log-lines (filter #(and id (str/includes? % id)) (str/split-lines (log-text)))]
        {:reply (str "Task: " sent " / Done: " done)
         :pass (boolean (and codex id handed reminded (str/starts-with? sent "SendAccepted.")
                             (str/starts-with? done "SendAccepted.") (= 1 (count lines)) (not second-reminder)))
         :watched (str "Task admitted " admitted ", deadline 60 s later; Codex flow " codex
                       " session holds Tag g1 " (boolean handed) "; Reminder 1 transcript stamps " (pr-str stamps)
                       " (" (count lines) " line); no Reminder 2 in the 75 s after Done " (not second-reminder)
                       "; Nexus log lines naming " id ": " (pr-str (vec log-lines)))}))]])

;; FLOW_CAPSULE_SKIP_CODEX=1: no Codex app-server is started, so every scenario
;; that needs a Codex seat (the codex-scenarios) is recorded SKIPPED, never pass.
(def skip-codex (= "1" (System/getenv "FLOW_CAPSULE_SKIP_CODEX")))

(def skipped-codex-scenarios
  (mapv (fn [[name _]]
          [name (fn [] {:reply "skipped" :pass false :skipped true
                        :watched "FLOW_CAPSULE_SKIP_CODEX=1: needs the Codex app-server, which was not started"})])
        codex-scenarios))

(def scenarios (vec (concat (subvec base-scenarios 0 11) night-scenarios
                            (if skip-codex skipped-codex-scenarios codex-scenarios)
                            (subvec base-scenarios 11))))

;; ---------------------------------------------------------------- handover
;; flow-capsule <flow package> --handover: the handover canary. The Codex Terra
;; seat, re-declared Maintained, keeps and drops sections of its handover,
;; is warned when its warning window opens, is refreshed into a successor
;; that is handed the Kept sections, and has its closed generation archived
;; to a git repository of the capsule's. Every reading is the real seat's:
;; its transcript, a CLI reply, the Nexus log, the archive's git log.
;; Context is measured only at a Stop; the window opens when
;; tokens*100 >= Refresh*(100-WarningShareLeft).

(def ho-seat "{ Mind test Terra }")
(def ho-other "{ Mind test Luna }")
(def ho-model codex-model)
(def ho-needle "canary-needle-b060be")
(def ho-every-minutes 1)
(def ho-share 20)
(def ho-high 200000)
(def ho-archive "/tmp/flow-0532-01e4a4-preflight/handover-archive")
(def ho-hook (str ho-archive "/.git/hooks/pre-commit"))
(def ho-hook-refused (str ho-archive "/.git/hook-refused"))
(def ho-hook-waiting (str ho-archive "/.git/hook-waiting"))
(def ho-hook-release (str ho-archive "/.git/hook-release"))
(def ho-bulk (str source-root "/bulk.txt"))
(def ho (atom {}))
(def retirement-observation (atom nil))

(declare keep-datom)
(defn ho-reminder []
  (str "flow '" (keep-datom "canary-memory" "Copy and fill this handover section." ho-needle) "'"))

(defn ho-policy [share]
  (str "Configure.HandoverPolicies.[ { " ho-model " " share " «" (ho-reminder) "» " ho-every-minutes " } ]"))

(defn ho-threshold [r]
  (str "Configure.Thresholds.[ { " ho-model " " r " } ]"))

(defn keep-datom [title description body]
  (str "Keep.{ " ho-seat " { «" title "» «" description "» «" body "» } }"))

(defn ho-pane [] (bound-pane (inspect ho-seat)))

(def observation-root (str private-root "/observations"))

(defn observer-argv [& args]
  (vec (concat [codex-observer "--codex" codex-client "--private-root" private-root
                "--socket" codex-socket]
               args)))

(defn observer-json
  "Runs one bounded observer query and returns its JSON object."
  [label & args]
  (let [{:keys [exit out err]} (apply run {:timeout 300000} (apply observer-argv args))]
    (keep-text! (str "handover-observer-" label ".stdout.json") out)
    (keep-text! (str "handover-observer-" label ".stderr.txt") err)
    (when-not (zero? exit)
      (throw (ex-info "Codex observer query failed" {:label label :exit exit :error (str/trim err)})))
    (json/parse-string out true)))

(defn observer-model-catalog [] (observer-json "models" "models"))

(defn observer-history [label thread-id]
  (observer-json label "history" "--thread-id"
                 (require-handover-identifier :native-thread thread-id)))

(defn history-user-turns
  "Every exact-thread userMessage, chronologically, preserving its full content
  array and projecting only content[type=text] for matching."
  [history]
  (vec (for [{:keys [turnId content]} (:userMessageContent history)]
         {:turn-id turnId
          :content content
          :text (str/join "\n" (keep #(when (= "text" (:type %)) (:text %)) content))})))

(defn user-turns [thread-id]
  (history-user-turns (observer-history (str "history-" thread-id "-" (System/nanoTime)) thread-id)))

(defn turns-holding [thread-id needle]
  (filterv #(str/includes? (:text %) needle) (user-turns thread-id)))

(defn observation-lines [path]
  (vec (keep #(try (json/parse-string % true) (catch Exception _ nil))
             (when (fs/exists? path) (str/split-lines (slurp path))))))

(defn record-turn-id [record]
  (or (get-in record [:params :turnId]) (get-in record [:params :turn :id])))

(defn correlated-token-signal
  "Correlates one exact action tag to one fresh user turn, its ordered native
  usage updates, and that same turn's successful completion. The final usage
  update before completion supplies last.inputTokens; it is not the total.
  Ambiguous target identity or incomplete ordering remains unobserved."
  [action-tag before-turn-ids after-turns records]
  (let [fresh (set (remove before-turn-ids (map :turn-id after-turns)))
        targets (set (for [turn after-turns
                           :when (and (fresh (:turn-id turn))
                                      (str/includes? (:text turn) action-tag))]
                       (:turn-id turn)))
        target (when (= 1 (count targets)) (first targets))
        indexed (map-indexed vector records)
        completions (vec (for [[index record] indexed
                               :when (and target (= "turn/completed" (:method record))
                                          (= target (record-turn-id record))
                                          (= "completed" (get-in record [:params :turn :status])))]
                           {:index index :record record}))
        completion (when (= 1 (count completions)) (first completions))
        signals (vec (for [[index record] indexed
                           :let [tokens (get-in record [:signal :lastInputTokens])]
                           :when (and target (= "thread/tokenUsage/updated" (:method record))
                                      (= target (record-turn-id record))
                                      (integer? tokens) (pos? tokens))]
                       {:index index :record record
                        :turnId target :lastInputTokens tokens
                        :timestamp (:timestamp record)}))
        before-completion (when completion (filterv #(< (:index %) (:index completion)) signals))
        all-before-completion (and completion (seq signals) (= (count signals) (count before-completion)))
        signal (when all-before-completion (last before-completion))]
    {:fresh-turn-ids fresh :target-turn-ids targets :completion completion
     :signals signals :signals-before-completion before-completion :signal signal}))

(defn matched-turn-events
  "Matches full-history user turns containing needle to one native event of
  method each, preserving history order. Missing or doubled events refuse the
  observation instead of assigning a discovery time."
  [needle turns records method]
  (let [matched (filterv #(str/includes? (:text %) needle) turns)
        events (mapv (fn [turn]
                       (let [found (filterv #(and (= method (:method %))
                                                  (= (:turn-id turn) (record-turn-id %)))
                                            records)]
                         (when (= 1 (count found))
                           {:turn turn :event (first found) :timestamp (:timestamp (first found))})))
                     matched)]
    (when (and (seq matched) (every? some? events)) events)))

(defn observed-turn!
  "Subscribes before action, records the exact thread's complete bounded
  notification interval, then returns action's result and the positive
  tokenUsage.last.inputTokens signal for the one newly observed user turn
  carrying tag. Other turns in the interval remain recorded. An absent or
  unmatched signal remains nil: it is never synthesized."
  [action-tag thread-id action]
  (fs/create-dirs observation-root)
  (let [before (set (map :turn-id (history-user-turns (observer-history (str action-tag "-before") thread-id))))
        log (str observation-root "/" action-tag ".jsonl")
        stdout (str observation-root "/" action-tag ".stdout")
        stderr (str observation-root "/" action-tag ".stderr")
        _ (doseq [path [log stdout stderr]]
            (when (fs/exists? path) (throw (ex-info "observer witness path already exists" {:path path}))))
        proc (p/process (observer-argv "observe" "--thread-id" thread-id "--duration" "90" "--log" log)
                        {:env (clean-env) :out :write :out-file (fs/file stdout)
                         :err :write :err-file (fs/file stderr) :in ""})
        subscribed (watch 20 #(and (fs/exists? stdout) (str/includes? (slurp stdout) "\"event\":\"subscribed\"")))
        _ (when-not subscribed
            (p/destroy-tree proc)
            (throw (ex-info "Codex observer did not subscribe before the measured turn" {:action-tag action-tag :stderr (when (fs/exists? stderr) (slurp stderr))})))
        result (action)
        finished (deref proc 110000 ::timeout)
        _ (when (= finished ::timeout)
            (p/destroy-tree proc)
            (throw (ex-info "Codex observation did not finish" {:action-tag action-tag})))
        _ (when-not (zero? (:exit finished))
            (throw (ex-info "Codex observation failed" {:action-tag action-tag :exit (:exit finished) :stderr (slurp stderr)})))
        after-history (observer-history (str action-tag "-after") thread-id)
        after (history-user-turns after-history)
        lines (observation-lines log)
        summary (last (filter #(= "observation-complete" (:event %)) lines))
        {:keys [fresh-turn-ids target-turn-ids completion signals signal]}
        (correlated-token-signal action-tag before after lines)]
    (keep-text! (str "handover-observer-" action-tag ".jsonl") (slurp log))
    (keep-text! (str "handover-observer-" action-tag ".stdout.txt") (slurp stdout))
    (keep-text! (str "handover-observer-" action-tag ".stderr.txt") (slurp stderr))
    (keep-text! (str "handover-observer-" action-tag "-history.json") (json/generate-string after-history {:pretty true}))
    {:result result :action-tag action-tag :thread-id thread-id :fresh-turn-ids fresh-turn-ids :target-turn-ids target-turn-ids
     :after-turns after :records lines :summary summary :completion completion :signals signals
     :turn-id (:turnId signal) :tokens (:lastInputTokens signal)
     :observation (if signal "OBSERVED" "UNOBSERVED")}))

(defn touch-tokens
  "The tokens a Kept or Dropped reply's Known touch carries; nil for Unknown."
  [reply]
  (some-> (first (heads-named "Known" (read-datom reply))) body items second str parse-long))

(defn window-open? [tokens r share] (and tokens r (>= (* tokens 100) (* r (- 100 share)))))

(defn sections-of
  "The sections a Handover reply carries: [title description body]."
  [reply]
  (let [d (read-datom reply)]
    (when (= "Handover" (head d))
      (vec (for [s (items (body d))] (mapv #(if (map? %) (:string %) (str %)) (items s)))))))

(defn handover-text [sections]
  (str/join "\n\n" (for [[t d b] sections] (str "## " t "\n" d "\n" b))))

(def ho-calls (str source-root "/calls"))

(defn ho-script!
  "A script that calls the package's client on the capsule's socket with
  one datom and writes its reply to a file; a seat runs it, so the call
  comes from that seat's pane. With wait-for, it first waits in the
  background, deaf to hangups, until that file exists."
  [tag client datom & [wait-for]]
  (let [sock (if (= client "flow") ordinary-socket meta-socket)
        dfile (str ho-calls "/" tag ".datom") out (str ho-calls "/" tag ".out") script (str ho-calls "/" tag ".sh")
        call (if (and handover-mode (= client "flow"))
               (str "\"" private-root "/bin/flow\" \"$d\" > " out ".tmp 2>&1; mv " out ".tmp " out)
               (str "\"$P/bin/" client "\" \"$(\"$P/bin/" client "-compose-invocation\" " sock " \"$d\")\" > " out ".tmp 2>&1; mv " out ".tmp " out))]
    (fs/create-dirs ho-calls)
    (spit dfile datom)
    (spit script (str "#!/bin/sh\nP=" @package "\nd=$(cat " dfile ")\n"
                      (if wait-for
                        (str "( trap '' HUP TERM; while [ ! -e " wait-for " ]; do sleep 1; done; " call " ) </dev/null >/dev/null 2>&1 &\n")
                        (str "trap '' HUP\n" call "\n"))))
    [script out]))

(defn type-into! [pane text] (herdr "pane" "run" pane text))

(defn ho-turn!
  "A prompt typed into the seat's pane, and the seat's next Stop awaited.
  Answers whether it rested."
  [flow-id text]
  (let [before (stops-of flow-id)]
    (type-into! (bound-pane (inspect ho-seat)) text)
    (watch 240 #(> (stops-of flow-id) before))))

(defn seat-call!
  "The seat at addr runs one ordinary client call itself, from its own
  pane: a prompt typed there asks it to run the call's script. Answers the
  reply, or nil when none was written; waits for the seat's Stop."
  [addr tag datom]
  (let [[script out] (ho-script! tag "flow" datom)
        flow-id (str (awake-flow addr))
        before (stops-of flow-id)]
    (type-into! (bound-pane (inspect addr))
                (str "OPERATOR-SCRIPT: Run the shell command sh " script " once, then answer with one word and stop. Tag " tag "."))
    (let [reply (when (watch 150 #(fs/exists? out)) (str/trim (slurp out)))]
      (watch 90 #(> (stops-of flow-id) before))
      reply)))

(defn bulk-order [tag]
  (str "OPERATOR-SCRIPT: Run the shell command cat " ho-bulk " once, then answer with one word and stop. Tag " tag "."))

(defn ho-git [& args] (apply run {} "git" "-C" ho-archive args))

(defn log-lines-with [& needles]
  (filterv (fn [l] (every? #(str/includes? l %) needles)) (str/split-lines (log-text))))

(defn retirement-address-text [address]
  (str "{ " (str/join " " (map str (items address))) " }"))

(defn retirement-key [d]
  (let [[address generation predecessor successor] (items d)]
    {:address (retirement-address-text address)
     :generation (some-> generation str parse-long)
     :predecessor (str predecessor)
     :successor (str successor)}))

(defn retirement-frame [line]
  (let [outer (read-datom line)
        inner (body outer)]
    (case (head outer)
      "RetirementObserved"
      (let [[address keys] (items inner)]
        {:kind :snapshot :address (retirement-address-text address)
         :keys (mapv retirement-key (items keys)) :raw line})

      "RetirementChanged"
      (case (head inner)
        "GenerationClosed" {:kind :closed :key (retirement-key (body inner)) :raw line}
        "GenerationRemoved" {:kind :removed :key (retirement-key (body inner)) :raw line}
        "ArchiveCommitted"
        (let [[key identity] (items (body inner))
              [commit path blob] (items identity)]
          {:kind :archive :key (retirement-key key)
           :identity {:commit (str commit) :path (str path) :blob (str blob)} :raw line})
        {:kind :unknown :raw line})

      "RetirementObservationFailed"
      {:kind :failed :failure (head inner) :detail (some-> inner body str) :raw line}

      {:kind :unknown :raw line})))

(defn retirement-verdict
  "Judges one fresh post-refusal subscription. The exact closed target must
  be its sole snapshot key, followed by archive evidence and removal for that
  key. Any other frame before removal makes the observation incomplete."
  [snapshot later predecessor successor expected-path git-evidence]
  (let [keys (:keys snapshot)
        key (first keys)
        target (and (= 1 (count keys)) (= predecessor (:predecessor key))
                    (= successor (:successor key)) (integer? (:generation key)))
        removed-index (first (keep-indexed #(when (= :removed (:kind %2)) %1) later))
        before-boundary (if removed-index (subvec (vec later) 0 (inc removed-index)) (vec later))
        kinds (mapv :kind before-boundary)
        [archive removed] before-boundary
        same-key (and archive removed (= key (:key archive) (:key removed)))
        identity (:identity archive)
        immutable-commit (and (boolean (re-matches #"[0-9a-f]{40}" (or (:commit identity) "")))
                              (zero? (or (:commit-exit git-evidence) -1))
                              (= (:commit identity) (:verified-commit git-evidence)))
        identity-exact (and immutable-commit (= expected-path (:path identity))
                            (zero? (or (:blob-exit git-evidence) -1))
                            (zero? (or (:content-exit git-evidence) -1))
                            (= (:blob identity) (:blob git-evidence))
                            (= (:content git-evidence) (:requested-content git-evidence)))]
    {:complete (boolean (and target (= [:archive :removed] kinds) same-key identity-exact))
     :key key :kinds kinds :target (boolean target) :same-key (boolean same-key)
     :immutable-commit (boolean immutable-commit) :identity-exact (boolean identity-exact)}))

(defn start-retirement-observation! []
  (let [query (str "Observe.Retirement." ho-seat)
        proc (p/process [(str @package "/bin/flow")
                         (client-argument @package "flow" ordinary-socket query)]
                        {:env (clean-env (marks @driver-pane)) :out :stream :err :string :in ""})
        reader (java.io.BufferedReader. (java.io.InputStreamReader. (:out proc)))
        first-line (deref (future (try (.readLine reader) (catch Exception _ nil))) 20000 nil)]
    (when-not first-line
      (p/destroy-tree proc)
      (throw (ex-info "retirement observation produced no attachment snapshot" {:query query})))
    (let [first-frame (retirement-frame first-line)]
      (when-not (and (= :snapshot (:kind first-frame)) (= ho-seat (:address first-frame)))
        (p/destroy-tree proc)
        (throw (ex-info "retirement observation attachment was not the selected Address snapshot"
                        {:query query :frame first-frame})))
      (let [frames (atom [first-frame])
            read-future (future
                          (loop []
                            (let [[outcome value] (try [:line (.readLine reader)]
                                                       (catch Exception e [:error e]))]
                              (cond
                                (= outcome :error)
                                (swap! frames conj {:kind :read-failed :error (.getMessage value)})

                                (nil? value)
                                (swap! frames conj {:kind :eof})

                                :else
                                (let [frame (try (retirement-frame value)
                                                 (catch Exception e
                                                   {:kind :parse-failed :error (.getMessage e) :raw value}))]
                                  (swap! frames conj frame)
                                  (when-not (contains? #{:parse-failed :unknown} (:kind frame))
                                    (recur))))))) ]
        (reset! retirement-observation {:query query :process proc :reader reader
                                        :frames frames :read-future read-future}))
      first-frame)))

(defn finish-retirement-observation! [timeout-ms]
  (let [{:keys [process reader frames]} @retirement-observation]
    (when-not (and process reader frames)
      (throw (ex-info "retirement observation was not attached before refresh" {})))
    (watch (/ timeout-ms 1000) #(some (fn [frame] (contains? #{:removed :failed :eof :read-failed :parse-failed :unknown} (:kind frame))) @frames))
    (let [result @frames]
      (p/destroy-tree process)
      result)))

(defn seven-line-contract-complete? [handover-only h0-diagnostic-only outcomes]
  (cond h0-diagnostic-only false
        handover-only (not-any? #(false? (:contract-pass %)) outcomes)
        :else true))

(defn h0-diagnostic-capture-complete? []
  (and @handover-finish-succeeded
       @handover-evidence-preserved
       @witness-dir
       (fs/regular-file? (str @witness-dir "/handover-H0-preservation.json"))
       (fs/regular-file? (str @witness-dir "/" h0-sampler-record-name))))

(defn h0-diagnostic-scenario []
  (when (fs/exists? ho-archive)
    (throw (ex-info "archive must be new" {:path ho-archive})))
  (let [catalog (observer-model-catalog)
              native-models (set (map :model (:models catalog)))
              catalogued (contains? native-models codex-model)
              _ (keep-text! "handover-H0-model-catalog.json"
                            (json/generate-string catalog {:pretty true}))
              metaflows-line (str/replace-first (configure-line "Metaflows")
                                                "Regular Default OnDemand" "Regular Default Maintained")
              metaflows (flow-meta metaflows-line)
              _ (fs/create-dirs ho-archive)
              git-init [(ho-git "init" "-q") (ho-git "config" "user.name" "flow-capsule")
                        (ho-git "config" "user.email" "flow-capsule@localhost")
                        (ho-git "commit" "-q" "--allow-empty" "-m" "Initial empty commit")]
              _ (spit ho-hook (str "#!/bin/sh\n"
                                   "if [ ! -e " ho-hook-refused " ]; then\n"
                                   "  : > " ho-hook-refused "\n"
                                   "  echo flow-capsule canary hook refuses once >&2\n"
                                   "  exit 1\n"
                                   "fi\n"
                                   ": > " ho-hook-waiting "\n"
                                   "while [ ! -e " ho-hook-release " ]; do sleep 0.1; done\n"))
              _ (fs/set-posix-file-permissions ho-hook "rwxr-xr-x")
              policy (flow-meta (ho-policy ho-share))
              archive (flow-meta (str "Configure.HandoverArchive." ho-archive))
              handover-count (flow-meta "Configure.HandoverCount.0")
              configuration (flow-meta "Configuration")
              launch-request (launch-query "Launch" ho-seat)
              rehearse-request (launch-query "Rehearse" ho-seat)
              rehearsal (flow rehearse-request)
              profile {:address ho-seat :brief brief :launch launch-request
                       :rehearse rehearse-request :configuration-lines (configure-lines)
                       :maintained-metaflows metaflows-line}]
          (keep-text! "handover-H0-diagnostic-prelaunch.json"
                      (json/generate-string {:profile profile :configuration configuration
                                             :rehearsal rehearsal :modelCatalogued catalogued}
                                            {:pretty true}))
          (start-h0-sampler!)
          (let [launched (flow launch-request)]
            (reset! handover-h0-refusals {:terra launched})
            (keep-text! "handover-H0-diagnostic-launch.txt" launched)
            {:reply launched
             :pass (str/starts-with? launched "Started.")
             :watched (str "one exact Terra Launch after Configuration and Rehearse; model catalogued "
                           catalogued "; configuration " configuration "; Metaflows " metaflows
                           "; policy " policy "; archive " archive "; count " handover-count
                           "; git init exits " (mapv :exit git-init))})))

(def h0-diagnostic-scenarios
  [["H0 diagnostic: exact Terra request rehearsed, launched once, and preserved before teardown"
    h0-diagnostic-scenario]])

(def handover-scenarios
  [["H0 setup: Terra re-declared Maintained; archive repo with a refusing pre-commit hook; policy, archive, count; Luna and Terra launched"
   (fn []
      (let [_ (when (fs/exists? ho-archive) (throw (ex-info "archive must be new" {:path ho-archive})))
            catalog (observer-model-catalog)
            native-models (set (map :model (:models catalog)))
            catalogued (contains? native-models codex-model)
            _ (keep-text! "handover-H0-model-catalog.json" (json/generate-string catalog {:pretty true}))
            metaflows (flow-meta (str/replace-first (configure-line "Metaflows")
                                                    "Regular Default OnDemand" "Regular Default Maintained"))
            _ (fs/create-dirs ho-archive)
            git-init [(ho-git "init" "-q") (ho-git "config" "user.name" "flow-capsule")
                      (ho-git "config" "user.email" "flow-capsule@localhost")
                      (ho-git "commit" "-q" "--allow-empty" "-m" "Initial empty commit")]
            _ (spit ho-hook (str "#!/bin/sh\n"
                                 "if [ ! -e " ho-hook-refused " ]; then\n"
                                 "  : > " ho-hook-refused "\n"
                                 "  echo flow-capsule canary hook refuses once >&2\n"
                                 "  exit 1\n"
                                 "fi\n"
                                 ": > " ho-hook-waiting "\n"
                                 "while [ ! -e " ho-hook-release " ]; do sleep 0.1; done\n"))
            _ (fs/set-posix-file-permissions ho-hook "rwxr-xr-x")
            _ (spit ho-bulk (str/join "\n" (for [i (range 600)] (str "line " i " of the canary bulk: alpha bravo charlie delta echo foxtrot"))))
            policy (flow-meta (ho-policy ho-share))
            archive (flow-meta (str "Configure.HandoverArchive." ho-archive))
            handover-count (flow-meta "Configure.HandoverCount.0")
            other-launch (launch ho-other)
            other-awake (watch 60 #(awake-flow ho-other))
            other-rested (when other-awake (watch 180 #(pos? (stops-of (str other-awake)))))
            test-launch (launch ho-seat)
            _ (reset! handover-h0-refusals {:luna other-launch :terra test-launch})
            flow-id (watch 60 #(awake-flow ho-seat))
            rested (when flow-id (watch 180 #(pos? (stops-of (str flow-id)))))
            declared (seat-declaration (configure-line "Metaflows") ho-seat)
            snapshot (flow-meta "Configuration")]
        (keep-text! "handover-H0-configuration.txt" snapshot)
        {:reply (str "Metaflows: " metaflows " / Policy: " policy " / Archive: " archive " / Count: " handover-count
                     " / Luna: " other-launch " / Terra: " test-launch)
         :pass (boolean (and (every? #(str/starts-with? % "Configured.") [metaflows policy archive handover-count])
                             (every? #(zero? (:exit %)) git-init)
                             catalogued
                             (str/includes? snapshot "Regular Default Maintained")
                             other-awake other-rested flow-id rested))
         :watched (str "native model/list names " codex-model " " catalogued
                       " (catalog membership is not entitlement); actual Terra launch/first Stop " (boolean rested)
                       "; git init exits " (mapv :exit git-init) "; Configuration names Terra Maintained "
                       (str/includes? snapshot "Regular Default Maintained") " (configure.datom declares " declared
                       "); Luna Awake " other-awake "; Terra Awake " flow-id ", first Stop " (boolean rested))}))]

   ["H1 Keep and Drop from the seat's own pane; NotOwner from another pane; UnknownCaller from the driver; Read before and after Drop"
    (fn []
      (let [pane (require-handover-identifier :seat-pane (ho-pane) ho-seat)
            other (require-handover-identifier :seat-pane (bound-pane (inspect ho-other)) ho-other)
            thread-id (handover-thread ho-seat)
            observed (observed-turn! "h1-keep-one" thread-id
                                     #(seat-call! ho-seat "h1-keep-one" (keep-datom "canary-one" "The first canary section." "Body one.")))
            k1 (:result observed)
            k2 (seat-call! ho-seat "h1-keep-two" (keep-datom "canary-two" "The section to drop." "Body two."))
            read1 (flow (str "Read." ho-seat))
            dropped (seat-call! ho-seat "h1-drop" (str "Drop.{ " ho-seat " «canary-two» }"))
            read2 (flow (str "Read." ho-seat))
            not-owner (seat-call! ho-other "h1-intruder" (keep-datom "intruder" "From the Luna pane." "x"))
            unknown (flow (keep-datom "intruder" "From the driver shell." "x") @driver-pane)
            read3 (flow (str "Read." ho-seat))
            t0 (:tokens observed)
            latest-touch (touch-tokens dropped)
            titles #(set (map first (sections-of %)))]
        (swap! ho assoc :h1-observed t0 :latest-touch latest-touch)
        {:reply (str "Keep one: " k1 " / Keep two: " k2 " / Read: " read1 " / Drop: " dropped " / Read: " read2
                     " / Keep from Luna pane: " not-owner " / Keep from driver: " unknown " / Read: " read3)
         :pass (boolean (and (str/starts-with? k1 "Kept.") (str/starts-with? k2 "Kept.")
                             (= "OBSERVED" (:observation observed)) t0 latest-touch
                             (= #{"canary-one" "canary-two"} (titles read1))
                             (str/starts-with? dropped "Dropped.")
                             (= #{"canary-one"} (titles read2))
                             (= not-owner "KeepRefused.NotOwner")
                             (= unknown "KeepRefused.UnknownCaller")
                             (= #{"canary-one"} (titles read3))))
         :watched (str "seat pane " pane ", Luna pane " other ", driver pane " @driver-pane
                       "; exact thread " thread-id ", measured turn " (:turn-id observed)
                       ", tokenUsage.last.inputTokens " t0 " (" (:observation observed) ")"
                       "; authoritative latest handover touch is Drop's Known touch " latest-touch
                       "; Read titles before Drop " (titles read1) ", after " (titles read2) ", after the refusals " (titles read3))}))]

   ["H2 warning when the window opens: Keep command and needle, first reminder is the warning, next one interval later, none for two intervals after a Keep inside the window"
    (fn []
      (let [flow-id (handover-flow ho-seat)
            thread-id (handover-thread ho-seat)
            rested (pos? (stops-of flow-id))
            latest-touch (:latest-touch @ho)
            baseline (observed-turn! "h2-warning-baseline" thread-id
                                     #(ho-turn! flow-id "Answer with one word and stop. Tag h2-warning-baseline."))
            tb (:tokens baseline)
            ;; Put the warning boundary immediately above the freshly
            ;; measured baseline, with refresh still another 20% away.
            r (some-> tb (* 100) (quot (- 100 ho-share)) inc)
            baseline-below (boolean (and tb r (not (window-open? tb r ho-share))))
            non-green-before (boolean (and latest-touch r (not (window-open? latest-touch r ho-share))))
            _baseline-gate (when-not (and (= "OBSERVED" (:observation baseline)) baseline-below non-green-before)
                             (throw (ex-info "latest touch and current context do not establish a non-green pre-window baseline"
                                             {:observation (:observation baseline) :baseline-tokens tb
                                              :latest-touch latest-touch :threshold r
                                              :baseline-below-window baseline-below
                                              :latest-touch-non-green non-green-before})))
            thresholds (flow-meta (ho-threshold r))
            measured (observed-turn! "h2-warning-crossing" thread-id
                                     #(ho-turn! flow-id "Answer with one word and stop. Tag h2-warning-crossing."))
            t1 (:tokens measured)
            below-refresh (boolean (and t1 r (< t1 r)))
            opened (window-open? t1 r ho-share)
            non-green (boolean (and latest-touch r (not (window-open? latest-touch r ho-share))))
            _crossing-gate (when-not (and (= "OBSERVED" (:observation measured)) below-refresh opened non-green)
                             (throw (ex-info "fresh Stop did not cross the non-green warning boundary below refresh"
                                             {:observation (:observation measured) :context-tokens t1
                                              :latest-touch latest-touch :threshold r :below-refresh below-refresh
                                              :warning-open opened :green (not non-green)})))
            ;; The crossing observer stayed subscribed for the cadence. Use
            ;; the native turn/started receipt times, never the later moment
            ;; at which history happened to be read.
            reminder-events (matched-turn-events ho-needle
                                                 (filterv #((:fresh-turn-ids measured) (:turn-id %))
                                                          (:after-turns measured))
                                                 (:records measured) "turn/started")
            first-event (first reminder-events)
            second-event (second reminder-events)
            first-turn (:turn first-event)
            second-turn (:turn second-event)
            first-at (:timestamp first-event)
            second-at (:timestamp second-event)
            _ (Thread/sleep 10000)
            kept (seat-call! ho-seat "h2-keep" (keep-datom "canary-window" "Kept inside the window." "Body window."))
            kept-at (java.time.Instant/now)
            touch (touch-tokens kept)
            green (window-open? touch r ho-share)
            _ (Thread/sleep (* 2 (+ 60 10) 1000))
            all (turns-holding thread-id ho-needle)
            after (filterv #(not (contains? #{(:turn-id first-turn) (:turn-id second-turn)} (:turn-id %))) all)
            keep-turns (turns-holding thread-id "flow 'Keep.")
            gap (when (and first-at second-at) (/ (- second-at first-at) 1000000000.0))]
        (swap! ho assoc :r r :t2 touch)
        (keep-text! "handover-H2-warning.txt" (json/generate-string {:threadId thread-id :turns all :keepAt (str kept-at)} {:pretty true}))
        {:reply (str "Thresholds " r ": " thresholds " / Keep from the seat: " kept)
         :pass (boolean (and (str/starts-with? thresholds "Configured.") rested baseline-below non-green-before
                             (= "OBSERVED" (:observation measured)) below-refresh opened non-green first-turn kept
                             (str/includes? (:text first-turn) "flow 'Keep.")
                             (= (:turn-id first-turn) (:turn-id (first keep-turns)))
                             (= 2 (count reminder-events)) second-turn gap (<= 55 gap 100)
                             (str/starts-with? (str kept) "Kept.") green (empty? after)
                             touch))
         :watched (str "latest handover touch " latest-touch ", pre-subscribed baseline " tb
                       ", R " r ", S " ho-share ", window from " (quot (* r (- 100 ho-share)) 100)
                       "; baseline below window " baseline-below ", latest touch non-green " non-green-before
                       "; pre-subscribed exact action h2-warning-crossing measured fresh Stop " t1
                       " tokens, below refresh " below-refresh ", window open " opened
                       "; latest touch remains non-green " non-green
                       "; first needle turn " (:turn-id first-turn) " native turn/started " first-at " holds flow 'Keep. "
                       (boolean (some-> first-turn :text (str/includes? "flow 'Keep.")))
                       ", first turn with a Keep command is it " (= (:turn-id first-turn) (:turn-id (first keep-turns)))
                       "; second " (:turn-id second-turn) " native turn/started " second-at ", " gap " s later; Keep at " kept-at " touch " touch
                       " green " green "; additional needle turns after the Keep interval: " (count after)
                       "; full exact-thread content kept as handover-H2-warning.txt")}))]

   ["H3 refresh at the Stop of a green seat past its threshold: successor handed the Kept sections last in its first prompt; the closed generation refuses the old flow"
    (fn []
      (let [old (handover-flow ho-seat)
            old-pane (require-handover-identifier :seat-pane (ho-pane) ho-seat)
            old-native (handover-thread ho-seat)
            read (flow (str "Read." ho-seat))
            sections (sections-of read)
            expected (handover-text sections)
            marker (str ho-calls "/h3-closed")
            [waiter old-out] (ho-script! "h3-old-keep" "flow" (keep-datom "after-close" "From the closed generation." "x") marker)
            armed (ho-turn! old (str "OPERATOR-SCRIPT: Run the shell command sh " waiter " once, then answer with one word and stop. Tag h3-arm."))
            cur (:t2 @ho)
            r3 (+ cur 300)
            thresholds (flow-meta (ho-threshold r3))
            measured (observed-turn! "h3-threshold" old-native #(ho-turn! old (bulk-order "h3-threshold")))
            sent (:result measured)
            crossed (boolean (and (:tokens measured) (>= (:tokens measured) r3)))
            successor (require-handover-identifier
                       :successor-flow
                       (watch 240 #(let [f (awake-flow ho-seat)] (when (and f (not= (str f) old)) (str f))))
                       ho-seat)
            ;; The successor runs under the ordinary threshold again.
            restored (flow-meta (ho-threshold ho-high))
            native (require-handover-identifier
                    :native-thread (watch 60 #(native-session (inspect ho-seat))) ho-seat)
            first-history (watch 120 #(let [h (observer-history "h3-successor-first" native)]
                                        (when (seq (history-user-turns h)) h)))
            handed (first (history-user-turns first-history))
            last-part (boolean (and handed (str/ends-with? (str/trim (:text handed)) (str/trim expected))))
            _ (spit marker "closed")
            old-keep (when (watch 30 #(fs/exists? old-out)) (str/trim (slurp old-out)))
            read-after (flow (str "Read." ho-seat))]
        (swap! ho assoc :pred old :successor successor)
        (keep-text! "handover-H3-first-prompt.json" (json/generate-string {:threadId native :history first-history :expectedHandover expected} {:pretty true}))
        {:reply (str "Thresholds " r3 ": " thresholds " / bulk turn rested " (boolean sent) " / restored: " restored
                     " / Keep by the old flow's waiter after the close: " old-keep " / Read after: " read-after)
         :pass (boolean (and (seq sections) crossed (= "OBSERVED" (:observation measured)) successor handed last-part
                             (= old-keep "KeepRefused.NotOwner")))
         :watched (str "old flow " old " baseline " cur " tokens, R " r3
                       "; pre-subscribed measured turn " (:turn-id measured) " tokenUsage.last.inputTokens " (:tokens measured)
                       " crossed " crossed "; successor " successor " exact thread " native
                       "; expected handover " (pr-str expected) "; its first prompt holds it " (boolean handed)
                       ", as its last part " last-part " (whole first-prompt history kept as handover-H3-first-prompt.json)"
                       "; the old flow (pane " old-pane ") armed a waiting Keep " (boolean armed)
                       ", run after the successor started, answered " (pr-str old-keep))}))]

   ["H4 closed generation publishes exact immutable Git identity before the same generation key is removed; failing pre-commit hook delays both completion events until retry"
    (fn []
      (let [pred (require-handover-identifier :predecessor-flow (:pred @ho) ho-seat)
            rel (str "Mind/test/Terra/" pred ".md")
            refused (watch 30 #(fs/exists? ho-hook-refused))
            publisher-waiting (watch 90 #(fs/exists? ho-hook-waiting))
            retry-lines (log-lines-with "handover archive")
            log-before (:out (ho-git "log" "--format=%h %s"))
            tracked-before (str/trim (:out (ho-git "ls-tree" "--name-only" "HEAD" "--" rel)))
            status-before (str/trim (:out (ho-git "status" "--porcelain")))
            exists-before (fs/exists? (str ho-archive "/" rel))
            live-before {:current (str (awake-flow ho-seat)) :handover (flow (str "Read." ho-seat))
                         :sibling (str (awake-flow ho-other)) :sibling-handover (flow (str "Read." ho-other))}
            retirement-snapshot (start-retirement-observation!)
            removed-at (System/currentTimeMillis)
            _ (spit ho-hook-release "release\n")
            committed (watch 30 #(str/includes? (:out (ho-git "log" "--format=%s")) "Archive handover"))
            secs (/ (- (System/currentTimeMillis) removed-at) 1000.0)
            log-after (:out (ho-git "log" "--stat" "--format=%h %s"))
            tracked-after (str/trim (:out (ho-git "ls-tree" "--name-only" "HEAD" "--" rel)))
            content (try (slurp (str ho-archive "/" rel)) (catch Exception _ nil))
            retirement-frames (finish-retirement-observation! 45000)
            later (subvec (vec retirement-frames) 1)
            emitted (some #(when (= :archive (:kind %)) (:identity %)) later)
            full-commit (and emitted (re-matches #"[0-9a-f]{40}" (:commit emitted)))
            commit-result (when full-commit (ho-git "rev-parse" "--verify" (str (:commit emitted) "^{commit}")))
            emitted-spec (when full-commit (str (:commit emitted) ":" (:path emitted)))
            blob-result (when emitted-spec (ho-git "rev-parse" "--verify" emitted-spec))
            content-result (when emitted-spec (ho-git "cat-file" "blob" emitted-spec))
            verdict (retirement-verdict retirement-snapshot later pred (:successor @ho) rel
                                        {:commit-exit (:exit commit-result)
                                         :verified-commit (some-> commit-result :out str/trim)
                                         :blob-exit (:exit blob-result)
                                         :blob (some-> blob-result :out str/trim)
                                         :content-exit (:exit content-result)
                                         :content (:out content-result) :requested-content content})
            live-after {:current (str (awake-flow ho-seat)) :handover (flow (str "Read." ho-seat))
                        :sibling (str (awake-flow ho-other)) :sibling-handover (flow (str "Read." ho-other))}
            live-preserved (= live-before live-after)
            archive-seen (boolean (and pred refused publisher-waiting (str/blank? tracked-before)
                                       (not (str/includes? log-before "Archive handover"))
                                       committed (= tracked-after rel) (< secs 15)))
            retirement-seen (boolean (and (= ho-seat (:address (:key verdict))) (:complete verdict) live-preserved))]
        (keep-text! "handover-H4-archive.txt" (str "before:\n" log-before "\nstatus: " status-before
                                                    "\n\nafter:\n" log-after "\n" rel ":\n" content
                                                    "\n\nNexus log:\n" (str/join "\n" retry-lines)))
        (keep-text! "handover-H4-retirement.txt"
                    (str (str/join "\n" (map #(or (:raw %) (pr-str %)) retirement-frames))
                         "\nverdict " (pr-str verdict) "\nlive-before " (pr-str live-before)
                         "\nlive-after " (pr-str live-after)))
        {:reply (str "git log before: " (str/trim log-before) " / after: " (str/replace (str/trim log-after) "\n" " | "))
         :pass (and archive-seen retirement-seen)
         :contract-pass retirement-seen
         :memory-retirement-seen retirement-seen
         :archive-seen archive-seen
         :watched (str "predecessor " pred "; Nexus log lines naming the handover archive " (count retry-lines)
                       ", first " (pr-str (first retry-lines)) "; before the hook went: file present " exists-before
                       ", tracked " (pr-str tracked-before) ", status " (pr-str status-before)
                       "; waiting hook released, \"Archive handover\" committed " (boolean committed) " " secs " s later, "
                       rel " tracked " (= tracked-after rel)
                       "; refusal observed " (boolean refused) ", next publisher held at the hook " (boolean publisher-waiting)
                       "; fresh ordinary retirement subscription exact sole closed key " (boolean (:target verdict))
                       ", changes through completion " (:kinds verdict) ", same key " (:same-key verdict)
                       ", emitted canonical immutable commit " (:immutable-commit verdict)
                       ", commit:path blob and bytes exact " (:identity-exact verdict)
                       ", live current handover and sibling fixture preserved " live-preserved
                       "; kept as handover-H4-archive.txt and handover-H4-retirement.txt")}))]

   ["H5 ordinary delivery past threshold until green: no refresh at Stop, an Order drained by Wake reaches the transcript, no Uncertain mark; a Keep inside the window, then the next Stop refreshes"
    (fn []
      (let [policy (flow-meta (ho-policy ho-share))
            flow-id (handover-flow ho-seat)
            thread-id (handover-thread ho-seat)
            first-stop (watch 180 #(pos? (stops-of flow-id)))
            baseline (observed-turn! "h5-baseline" thread-id #(ho-turn! flow-id (bulk-order "h5-baseline")))
            push1 (:result baseline)
            tp (:tokens baseline)
            r5 (+ tp 300)
            thresholds (flow-meta (ho-threshold r5))
            measured (observed-turn! "h5-threshold" thread-id #(ho-turn! flow-id (bulk-order "h5-threshold")))
            rested (:result measured)
            tq (:tokens measured)
            past (boolean (and tq (>= tq r5)))
            _ (Thread/sleep 15000)
            kept-awake (= (str (awake-flow ho-seat)) flow-id)
            kept-lines (log-lines-with flow-id "kept at its Stop")
            tag "canary-drain-h5"
            drained (seat-call! ho-other "h5-send" (str "Send.{ " ho-seat " Soft.Order.«Answer with one word and stop. Tag " tag ".» }"))
            reached (watch 90 #(first (turns-holding thread-id tag)))
            deliveries (flow-meta (str "ReadQueueDeliveries." ho-seat))
            uncertain (str/includes? deliveries "Uncertain")
            refused-lines (log-lines-with "PastThreshold")
            ;; The seat keeps from its own pane in a turn of its own: that
            ;; turn's Stop is the next Stop after the Keep.
            kept (str (seat-call! ho-seat "h5-keep" (keep-datom "canary-green" "Kept past the threshold." "Body green.")))
            green (window-open? (touch-tokens kept) r5 ho-share)
            successor (watch 240 #(let [f (awake-flow ho-seat)] (when (and f (not= (str f) flow-id)) (str f))))
            restored [(flow-meta (ho-threshold ho-high)) (flow-meta (ho-policy ho-share))]
            delivered (boolean (and reached (not uncertain)))]
        (swap! ho assoc :h5-successor successor)
        (keep-text! "handover-H5-queue-deliveries.txt" deliveries)
        {:reply (str "Policy S " ho-share ": " policy " / Thresholds " r5 ": " thresholds " / Send by the Luna seat: " drained
                     " / ReadQueueDeliveries: " deliveries " / Keep: " kept " / restored: " restored)
         :pass (boolean (and first-stop push1 (= "OBSERVED" (:observation baseline))
                             rested (= "OBSERVED" (:observation measured)) past kept-awake delivered
                             (str/starts-with? kept "Kept.") green successor))
         :watched (str "flow " flow-id " at " tp " tokens, R " r5 "; after the push " tq " (past " past
                       ", exact measured turn " (:turn-id measured) ")"
                       "; not refreshed at that Stop: still Awake " kept-awake ", log " (pr-str (last kept-lines))
                       "; ordinary Order by Wake " (if reached "seen" "not seen") " in the transcript"
                       ", Uncertain mark " (if uncertain "seen" "not seen") ", PastThreshold log lines " (count refused-lines)
                       (when (seq refused-lines) (str " " (pr-str (last refused-lines))))
                       " => delivery past threshold " (if delivered "seen" "not seen")
                       "; the seat's own Keep touch green " green
                       ", refreshed at that turn's Stop as " (or successor "not seen"))}))]

   ["H6 an ordered Refresh bypasses the gate: a non-green seat refreshed from its own pane, then from meta"
    (fn []
      (let [f1 (handover-flow ho-seat)
            rested (watch 180 #(pos? (stops-of f1)))
            glance (flow (str "Glance." ho-seat))
            own (seat-call! ho-seat "h6-refresh" (str "Refresh." ho-seat))
            f2 (require-handover-identifier
                :successor-flow
                (watch 180 #(let [f (awake-flow ho-seat)] (when (and f (not= (str f) f1)) (str f))))
                ho-seat)
            rested2 (when f2 (watch 180 #(pos? (stops-of f2))))
            by-meta (flow-meta (str "Refresh." ho-seat))
            f3 (require-handover-identifier
                :successor-flow
                (watch 180 #(let [f (awake-flow ho-seat)] (when (and f f2 (not= (str f) f2)) (str f))))
                ho-seat)]
        {:reply (str "Glance: " glance " / Refresh from its pane: " own " / Refresh from meta: " by-meta)
         :pass (boolean (and rested (or (nil? own) (str/starts-with? own "Refreshed.")) f2
                             (str/starts-with? by-meta "Refreshed.") f3))
         :watched (str "flow " f1 " (no Keep of its own, so not green) rested " (boolean rested)
                       "; its own pane's Refresh (reply " (pr-str own) ", nil when the closing pane took the client) started " (or f2 "nothing") ", rested " (boolean rested2)
                       "; meta Refresh started " (or f3 "nothing"))}))]

   ["H7 empty-queue threshold crossing: after a pre-subscribed measured Stop, a Soft message reaches the same native thread before any green"
    (fn []
      (let [flow-id (handover-flow ho-seat)
            rested (watch 180 #(pos? (stops-of flow-id)))
            thread-id (handover-thread ho-seat)
            baseline (observed-turn! "h7-baseline" thread-id #(ho-turn! flow-id (bulk-order "h7-baseline")))
            tp (:tokens baseline)
            r7 (+ tp 300)
            thresholds (flow-meta (ho-threshold r7))
            before-snapshot (verified-awake-current ho-seat)
            empty-before (and (= flow-id (:flow before-snapshot)) (vector? (:queue before-snapshot))
                              (empty? (:queue before-snapshot)))
            measured (observed-turn! "h7-threshold-empty-queue" thread-id
                                     #(ho-turn! flow-id (bulk-order "h7-threshold-empty-queue")))
            tq (:tokens measured)
            crossed (boolean (and tq (>= tq r7)))
            stop-snapshot (verified-awake-current ho-seat)
            empty-at-stop (and (= flow-id (:flow stop-snapshot)) (vector? (:queue stop-snapshot))
                               (empty? (:queue stop-snapshot)))
            _ (Thread/sleep 15000)
            same-before-send (= flow-id (str (awake-flow ho-seat)))
            history-before (observer-history "h7-before-soft" thread-id)
            before-ids (set (map :turn-id (history-user-turns history-before)))
            tag "canary-empty-queue-h7"
            sent (seat-call! ho-other "h7-send" (str "Send.{ " ho-seat " Soft.Order.«Answer with one word and stop. Tag " tag ".» }"))
            reached (watch 120 #(first (filter (fn [turn] (and (not (before-ids (:turn-id turn)))
                                                                (str/includes? (:text turn) tag)))
                                               (user-turns thread-id))))
            same-when-reached (= flow-id (str (awake-flow ho-seat)))
            deliveries (flow-meta (str "ReadQueueDeliveries." ho-seat))
            uncertain (str/includes? deliveries "Uncertain")]
        (keep-text! "handover-H7-evidence.json"
                    (json/generate-string {:threadId thread-id :flowId flow-id :threshold r7
                                           :baseline baseline :measured measured :beforeSnapshot before-snapshot
                                           :stopSnapshot stop-snapshot :emptyBefore empty-before
                                           :emptyAtStop empty-at-stop :sameBeforeSend same-before-send
                                           :reached reached :sameWhenReached same-when-reached
                                           :queueDeliveries deliveries} {:pretty true}))
        {:reply (str "Thresholds " r7 ": " thresholds " / Soft send: " sent " / deliveries: " deliveries)
         :pass (boolean (and rested (= "OBSERVED" (:observation baseline))
                             (= "OBSERVED" (:observation measured)) crossed empty-before empty-at-stop
                             same-before-send (str/starts-with? sent "SendAccepted.") reached
                             same-when-reached (not uncertain)))
         :watched (str "exact thread " thread-id ", flow " flow-id "; baseline turn " (:turn-id baseline)
                       " tokenUsage.last.inputTokens " tp "; R " r7 "; empty-queue measured turn " (:turn-id measured)
                       " tokens " tq ", crossed " crossed "; queue empty before/at Stop " empty-before "/" empty-at-stop
                       "; no Keep was made in this generation; same flow before send " same-before-send
                       "; fresh Soft user turn " (:turn-id reached) " reached same native thread before refresh " same-when-reached
                       "; Uncertain " uncertain)}))]])

;; ---------------------------------------------------------------- takeover
;; flow-capsule --takeover <old flow package> <new flow package>: the bie3.6
;; transition (procedure §6) inside the capsule's own Herdr session, sockets
;; and store. The old Nexus serves Awake seats and queued work; a new store
;; is seeded on a private socket past the old ids; a guard holds the new
;; Nexus off while the old one answers; the old one is stopped by hand and
;; the new one started; the seats are re-bound by BindSeat typed into their
;; panes. The old Nexus is restored by its recorded command after a success
;; and after the new store is refused. Arm A keeps the sender silent through
;; the window (the gate); Arm B, the control, keeps it sending.

(def codex-secondary "{ Mind test Sol }")
(def asleep-primary "{ Psyche test Fable }")
;; The margin ruled for the counters: the old maximum plus this.
(def counter-margin 10000)
;; The night's seat count the window is extrapolated to.
(def night-seats 21)
(def seed-socket (str capsule-runtime "/seed/flow.sock"))
(def seed-meta-socket (str capsule-runtime "/seed/flow-meta.sock"))

(defn split-replay
  "Datoms split as the production replay splits them: at a newline outside
  guillemets, blank pieces dropped."
  [text]
  (let [close (fn [out current] (if (str/blank? (str current)) out (conj out (str current))))]
    (loop [[c & more :as cs] (seq text) depth 0 current (StringBuilder.) out []]
      (cond
        (empty? cs) (close out current)
        (and (= c \newline) (zero? depth)) (recur more depth (StringBuilder.) (close out current))
        :else (recur more (case c \« (inc depth) \» (dec depth) depth) (.append current c) out)))))

(defn- append-entries
  "A vector-bodied Configure line with entries added before its closing bracket."
  [line entries]
  (str/replace line #"\s*\]\s*$" (java.util.regex.Matcher/quoteReplacement (str " " (str/join " " entries) " ]"))))

(defn takeover-configuration
  "The takeover's declarations from the capsule's Configure lines: every
  seat it keeps Awake is Maintained, a Codex Sol on the Codex
  Haiku's model and an OnDemand Fable that stays asleep are added.
  :old omits DefaultMinutes, which the old Nexus does not know; :replay is
  every line, in the file's order."
  [lines]
  (let [codex-model (some #(second (re-find #"\{ Terra (\S+) \}" %)) lines)
        line-of (fn [line]
                  (cond
                    (str/starts-with? line "Configure.Metaflows.")
                    (append-entries (str/replace line " OnDemand «" " Maintained «")
                                    [(str "{ " codex-secondary " None [ spirit:core intent:models ] [] Simplified Explicit.[] Maintained «» [] }")
                                     (str "{ " asleep-primary " None [ spirit:core intent:models ] [] Regular Default OnDemand «» [] }")])
                    (and codex-model (str/starts-with? line "Configure.Models."))
                    (append-entries line [(str "{ Sol " codex-model " }")])
                    (str/starts-with? line "Configure.LeaveOuts.")
                    (if-let [entry (re-find #"\{ \{ Mind test Terra \} Codex \[[^\]]*\] \}" line)]
                      (append-entries line [(str/replace entry "Terra" "Sol")])
                      line)
                    :else line))
        replay (mapv line-of lines)]
    {:old (->> replay
               (remove #(str/starts-with? % "Configure.DefaultMinutes."))
               (mapv #(if (str/starts-with? % "Configure.Metaflows.")
                        (str/replace % #"(\{ \{ (?:Psyche|Mind|Field) [^}]+ \}) None " "$1 ")
                        %)))
     :replay replay}))

(def id-pattern #"logical-delivery-\d{20}|queue-attempt-[0-9a-f]{16}|send-outcome-[0-9a-f]{16}")

(defn issued-ids
  "Every logical-delivery, queue-attempt and send-outcome id in text."
  [text] (set (re-seq id-pattern (or text ""))))

(defn id-number
  "An id's family and number: a logical delivery in decimal, the others in hex."
  [id]
  (let [[_ family digits] (re-matches #"(logical-delivery|queue-attempt|send-outcome)-([0-9a-f]+)" id)]
    [(keyword family) (if (= family "logical-delivery") (parse-long digits) (Long/parseLong digits 16))]))

(defn old-maxima [ids]
  (reduce (fn [m id] (let [[family n] (id-number id)] (update m family max n)))
          {:logical-delivery 0 :queue-attempt 0 :send-outcome 0} ids))

(defn counters-datom
  "Configure.Counters at each old maximum plus the margin, in the ruled order."
  [{:keys [logical-delivery queue-attempt send-outcome]} margin]
  (str "Configure.Counters.{ " (+ logical-delivery margin) " " (+ queue-attempt margin) " "
       (+ send-outcome margin) " }"))

(defn configuration-counters
  "The three counters a Configuration reply ends with; nil for any other reply."
  [reply]
  (let [d (read-datom reply)
        container (loop [x d] (if (and (map? x) (:head x)) (recur (:body x)) x))
        tail (when (map? container) (last (items container)))
        values (when (and (map? tail) (= \{ (:enclosure tail))) (map str (items tail)))]
    (when (and (= 3 (count values)) (every? #(re-matches #"\d+" %) values))
      (mapv parse-long values))))

(defn reissued
  "The ids both Nexuses issued."
  [old-ids new-ids] (vec (sort (filter (set old-ids) new-ids))))

(defn below-counters
  "The ids below their family's seeded counter."
  [ids [logical queue outcome]]
  (let [floor {:logical-delivery logical :queue-attempt queue :send-outcome outcome}]
    (vec (sort (filter #(let [[family n] (id-number %)] (< n (floor family))) ids)))))

(defn owner-answers?
  "The guard's rule: a Nexus owns a socket unless its client found no
  socket there or was refused the connection; a reply the client cannot
  read is an owner still."
  [reply]
  (not (and (str/starts-with? reply "exit ")
            (or (str/includes? reply "(os error 2)") (str/includes? reply "(os error 111)")))))

;; The seed's order, as the Nexus allows it. A full Nexus opens only a store
;; that holds the Nexus declaration (FlowStore::open_configured), and a
;; configuration-only Nexus refuses Counters NotRunning; Counters is taken
;; only while no logical delivery, queue attempt or send outcome is
;; recorded, which a full Nexus on a store declaring no metaflow leaves so.
(defn seed-plan
  "The seed's steps, each {:via :configure-only|:running :datom}: the Nexus
  declared configuration-only; then a full Nexus given Counters, the lower
  one and the Configuration read; then the rest of the replay
  configuration-only, in the file's order. Nil without a Nexus declaration."
  [replay counters lowered]
  (let [nexus? #(str/starts-with? % "Configure.Nexus.")]
    (when-let [nexus (first (filter nexus? replay))]
      (vec (concat [{:via :configure-only :datom nexus}]
                   (for [datom [counters lowered "Configuration"]] {:via :running :datom datom})
                   (for [datom (remove nexus? replay)] {:via :configure-only :datom datom}))))))

(defn declared-start
  "The declared unit's start: its guard, as ExecStartPre, reads the probe;
  while any socket has an owner it refuses and start! is never called."
  [probe start!]
  (if (some owner-answers? (vals probe)) :refused (start!)))

(defn shell-quote [text]
  (str "'" (str/replace text "'" "'\"'\"'") "'"))

(defn rebind-lines
  "One line per seat pane, as the procedure's C7 makes them from the pane
  list: the trackers (tracker? of `Aspect topic Layer`) first, then the rest
  in pane order; a pane with no seat label is not typed to."
  [pane-list pkg tracker?]
  (let [rows (keep (fn [{:keys [pane_id label]}]
                     (when-let [[_ aspect topic model flow-id]
                                (re-matches #"\{ (\w+) (\S+) (\w+) ([0-9a-f]{6}) \}" (str label))]
                       (let [words (str aspect " " topic " " model)]
                         (let [query (str "BindSeat.{ " words " }")
                               {:keys [exit out err]}
                               (run {} (str pkg "/bin/flow-compose-invocation") ordinary-socket query)]
                           (when-not (zero? exit)
                             (throw (ex-info "typed rebind composition failed"
                                             {:package pkg :error (str/trim err)})))
                           {:pane pane_id :flow-id flow-id :address (str "{ " words " }")
                            :line (str "! " pkg "/bin/flow " (shell-quote (str/trim out)))
                            :tracker (boolean (tracker? words))}))))
                   pane-list)]
    (mapv #(dissoc % :tracker) (concat (filter :tracker rows) (remove :tracker rows)))))

(defn address-text [a] (str "{ " (str/join " " (map str (items a))) " }"))

(defn awake-seats
  "Each Awake metaflow's FlowId by Address, from a Listed reply."
  [listed]
  (into {} (for [m (items (body listed))
                 :let [[a state] (items m)]
                 :when (= (head state) "Awake")]
             [(address-text a) (str (body state))])))

(defn changed-seats
  "The Addresses of before whose seat is not Awake under the same FlowId after."
  [before after]
  (vec (sort (for [[a flow-id] before :when (not= flow-id (get after a))] a))))

(defn window-measure
  "The window from the hand stop, in seconds: ready, end, each pane's mean
  type-to-Bound, the readback, and the extrapolation to the night's seats."
  [{:keys [start ready end pane-ms readback-ms seats]}]
  (let [span #(when (and %1 %2) (/ (- %2 %1) 1000.0))
        mean (when (seq pane-ms) (/ (reduce + pane-ms) (count pane-ms) 1000.0))
        readback (when readback-ms (/ readback-ms 1000.0))]
    {:ready-s (span start ready) :end-s (span start end) :mean-pane-s mean :readback-s readback
     :extrapolated-s (when (and mean readback) (+ (* mean seats) readback))}))

;; The takeover's processes and readings.

(def old-package (atom nil))
(def new-package (atom nil))
;; What the takeover's steps hand one another.
(def takeover (atom {}))

(defn now [] (System/currentTimeMillis))
(defn iso [ms] (str (java.time.Instant/ofEpochMilli ms)))

(defn package-env [pkg extra]
  (clean-env (merge {"PATH" (str pkg "/bin:" (System/getenv "PATH"))} extra)))

(defn meta-at
  "One meta call by pkg's client on socket, from no pane: the owner."
  [pkg socket datom]
  (reply-text (run {:env (package-env pkg {})} (str pkg "/bin/flow-meta")
                   (client-argument pkg "flow-meta" socket datom))))

(defn guard-probe
  "The guard's reading, as the declared unit's ExecStartPre would take it:
  each of the capsule's Flow sockets asked by pkg's client."
  [pkg]
  {:ordinary (reply-text (run {:env (package-env pkg (marks @driver-pane))}
                              (str pkg "/bin/flow")
                              (client-argument pkg "flow" ordinary-socket "Metaflows")))
   :meta (meta-at pkg meta-socket "Configuration")})

(defn guard-refuses? [probe] (boolean (some owner-answers? (vals probe))))

(defn nexus-command
  "The command a Nexus of pkg is started by on the capsule's sockets."
  [pkg store]
  {:argv [(str pkg "/bin/flow-nexus") (start-text ordinary-socket meta-socket store)] :env (package-env pkg {})})

(defn start-command! [{:keys [argv env]} log]
  (let [proc (p/process argv {:env env :out :append :out-file (fs/file log)
                              :err :append :err-file (fs/file log) :in ""})]
    (swap! takeover-nexuses conj proc)
    proc))

(defn stop-process! [proc]
  (if (p/alive? proc)
    (do (p/destroy proc) (:exit (deref proc 20000 {:exit "still running"})))
    (:exit @proc)))

(defn serves?
  "Whether the Nexus process comes to answer Metaflows on the capsule's
  socket: :serving, :exited, or nil after 30 s."
  [proc]
  (watch 30 #(cond (not (p/alive? proc)) :exited
                   (owner-answers? (flow "Metaflows")) :serving)))

(defn digest [path]
  (when (fs/exists? path)
    (let [md (java.security.MessageDigest/getInstance "SHA-256")]
      (apply str (map #(format "%02x" (bit-and % 0xff))
                      (.digest md (java.nio.file.Files/readAllBytes (fs/path path))))))))

(defn state-reads
  "What the package in charge answers: Metaflows, Configuration,
  ReadSendOutcomes and each Address's ReadQueueDeliveries, and every id in them."
  []
  (let [reply (flow "Metaflows")
        listed (read-datom reply)
        addresses (mapv #(address-text (first (items %))) (items (body listed)))
        queues (into (sorted-map) (for [a addresses] [a (flow-meta (str "ReadQueueDeliveries." a))]))
        outcomes (flow-meta "ReadSendOutcomes")]
    {:metaflows reply :awake (awake-seats listed) :queues queues :outcomes outcomes
     :configuration (flow-meta "Configuration")
     :ids (issued-ids (str/join " " (cons outcomes (vals queues))))}))

(defn observe-deliveries
  "The first frame of the sender's ObserveSends, read from pane and then
  closed: the state of every logical delivery the sender sent. Nil when no
  frame comes in 20 s."
  [pane]
  (let [proc (p/process [(str @package "/bin/flow")
                         (client-argument @package "flow" ordinary-socket "ObserveSends.None")]
                        {:env (clean-env (marks pane))
                         :out :stream :err :string :in ""})
        line (deref (future (try (.readLine (java.io.BufferedReader. (java.io.InputStreamReader. (:out proc))))
                                 (catch Exception _ nil)))
                    20000 nil)]
    (p/destroy-tree proc)
    line))

(defn old-reads!
  "The old Nexus's reads; every id in them is kept as one the old Nexus
  issued. The logical deliveries' states are read through the sender's
  ObserveSends: ReadQueueDeliveries lists only unsettled attempts."
  []
  (let [reads (state-reads)
        frame (observe-deliveries (sender-pane))
        reads (assoc reads :deliveries-frame frame :deliveries (delivery-states frame))]
    (swap! takeover update :old-ids (fnil into #{}) (:ids reads))
    reads))

(defn keep-reads! [prefix reads]
  (keep-text! (str prefix ".metaflows") (:metaflows reads))
  (keep-text! (str prefix ".configuration") (:configuration reads))
  (keep-text! (str prefix ".sendoutcomes") (:outcomes reads))
  (keep-text! (str prefix ".queue") (str/join "\n" (for [[a q] (:queues reads)] (str a " " q))))
  (when (contains? reads :deliveries-frame) (keep-text! (str prefix ".deliveries") (str (:deliveries-frame reads))))
  (keep-text! (str prefix ".ids") (str/join "\n" (sort (:ids reads)))))

(defn pane-of-flow [flow-id]
  (some #(when (str/ends-with? (str (:label %)) (str " " flow-id " }")) (:pane_id %)) (panes)))

(defn start-sender!
  "The trackers' cron, stood in for: every 15 s a Wake Notice to the
  Haiku and the Sonnet, sent from pane by the client in charge."
  [pane]
  (let [running (atom true)
        sent (atom [])]
    (reset! sender {:running running :sent sent
                    :future (future
                              (while @running
                                (doseq [addr [quaternary tertiary-address]]
                                  (swap! sent conj {:at (now) :to addr
                                                    :reply (send-to addr "Wake" "Soft.Notice.«beat. Answer with one word and stop.»" pane)}))
                                (let [until (+ (now) 15000)]
                                  (while (and @running (< (now) until)) (Thread/sleep 100)))))})))

(defn stop-sender!
  "Stops the sender loop; every send it made."
  []
  (when-let [{:keys [running sent] :as loop-state} @sender]
    (reset! running false)
    (deref (:future loop-state) 30000 nil)
    (reset! sender nil)
    @sent))

(defn start-poller!
  "Counts the session's panes every second."
  []
  (let [running (atom true)
        counts (atom [])]
    (reset! poller {:running running :counts counts
                    :future (future (while @running
                                      (swap! counts conj [(now) (count (panes))])
                                      (Thread/sleep 1000)))})))

(defn stop-poller!
  "Stops the pane poll; every count it took."
  []
  (when-let [{:keys [running counts] :as poll} @poller]
    (reset! running false)
    (deref (:future poll) 5000 nil)
    (reset! poller nil)
    @counts))

(defn seed!
  "A new store seeded on a private socket, in the seed plan's order: the
  Nexus declared through a configuration-only Nexus; a full Nexus of its own
  given Counters at the old maxima plus the margin, a lower Counters
  (refused), and the Configuration read, then stopped; the rest of the
  replay through configuration-only Nexuses; then flow-store-check."
  [label maxima log]
  (let [pkg @new-package
        store (str state-root "/" label "/flow.sema")
        _ (fs/create-dirs (fs/parent store))
        _ (fs/create-dirs (fs/parent seed-meta-socket))
        start (start-text seed-socket seed-meta-socket store)
        counters (counters-datom maxima counter-margin)
        lowered (counters-datom maxima (dec counter-margin))
        replay (split-replay (str/join "\n" (:replay (:configuration @takeover))))
        plan (seed-plan replay counters lowered)
        configure-only! (fn [datom] (let [[reply exit] (configure-one! datom log pkg start seed-meta-socket)]
                                      {:via :configure-only :datom datom :reply reply :exit exit}))
        running! (fn [steps]
                   (let [proc (start-command! {:argv [(str pkg "/bin/flow-nexus") start] :env (package-env pkg {})} log)
                         up (watch 30 #(cond (not (p/alive? proc)) :exited
                                             (owner-answers? (meta-at pkg seed-meta-socket "Configuration")) :serving))
                         replies (mapv (fn [{:keys [datom]}]
                                         {:via :running :datom datom :up up
                                          :reply (if (= up :serving) (meta-at pkg seed-meta-socket datom) "not serving")})
                                       steps)]
                     (conj replies {:via :stop :up up :exit (stop-process! proc)})))
        done (loop [[step & more :as steps] plan out []]
               (cond
                 (empty? steps) out
                 (= :running (:via step)) (let [batch (vec (take-while #(= :running (:via %)) steps))]
                                            (recur (drop (count batch) steps) (into out (running! batch))))
                 :else (recur more (conj out (configure-only! (:datom step))))))
        reply-of (fn [datom] (some #(when (and (= :running (:via %)) (= datom (:datom %))) (:reply %)) done))
        counters-reply (str (reply-of counters))
        lowered-reply (str (reply-of lowered))
        configuration (str (reply-of "Configuration"))
        declared (filter #(= :configure-only (:via %)) done)
        stop (some #(when (= :stop (:via %)) %) done)
        check (reply-text (run {:env (package-env pkg {})} (str pkg "/bin/flow-store-check") (str "{ «" store "» }")))
        expected (mapv #(+ (get maxima %) counter-margin) [:logical-delivery :queue-attempt :send-outcome])
        family #(or (second (re-find #"^Configure\.([A-Za-z]+)" %)) %)
        seed {:store store :counters counters :expected expected :digest (digest store)
              :ok (boolean (and plan (= :serving (:up stop))
                                (str/starts-with? counters-reply "Configured.")
                                (str/includes? lowered-reply "Lowered")
                                (= (count replay) (count declared))
                                (every? #(str/starts-with? (:reply %) "Configured.") declared)
                                (= expected (configuration-counters configuration))
                                (str/starts-with? check "Opens.")))
              :text (str "seed plan " (if plan (pr-str (mapv #(str (name (:via %)) " " (family (:datom %))) plan)) "none: no Nexus declaration")
                         " / " (str/join " / " (for [{:keys [via datom reply up exit]} done]
                                                 (case via
                                                   :stop (str "full seed Nexus " (name (or up :silent)) ", stopped, exit " exit)
                                                   (str (family datom) " (" (name via) ") -> " reply))))
                         " / Configuration counters " (configuration-counters configuration)
                         " / flow-store-check -> " check)}]
    (keep-text! (str "takeover-" label "-seed.txt") (:text seed))
    (keep-text! (str "takeover-" label "-seed.configuration") configuration)
    seed))

(defn seed-step
  "The old maxima read again now, then a new store seeded past them."
  [label]
  (fn []
    (let [reads (old-reads!)
          maxima (old-maxima (:ids reads))
          seed (seed! label maxima @nexus-log)]
      (keep-reads! (str "takeover-" label "-before-seed") reads)
      (swap! takeover assoc :seed seed)
      {:reply (:text seed)
       :pass (:ok seed)
       :watched (str "old maxima " maxima " from " (count (:ids reads)) " ids; store " (:store seed)
                     " seeded on " seed-meta-socket ", sha256 " (:digest seed))})))

(defn guard-step
  "Activate first: the guarded start is tried as Restart=on-failure would,
  three times two seconds apart, and refused each time while the old Nexus
  answers; the seeded store is not touched."
  []
  (fn []
    (let [{:keys [seed old-proc]} @takeover
          probes (vec (for [_ (range 3)] (let [probe (guard-probe @new-package)] (Thread/sleep 2000) probe)))
          refused (every? guard-refuses? probes)
          untouched (= (:digest seed) (digest (:store seed)))
          answering (owner-answers? (flow "Metaflows"))]
      {:reply (str "guard readings " (pr-str probes))
       :pass (boolean (and refused untouched answering (p/alive? old-proc)))
       :watched (str "three guarded starts " (if refused "all refused" "not all refused")
                     "; new store " (if untouched "unchanged" "changed") "; old Nexus PID " (.pid (:proc old-proc))
                     (if answering " answers" " does not answer") " Metaflows")})))

(defn hand-switch!
  "The old Nexus's last reads, its hand stop (the window opens), the guard
  read again, and the new Nexus started on the seeded store; with silent,
  the sender is stopped just before the stop."
  [label silent]
  (let [{:keys [seed old-proc]} @takeover
        sent (when silent (stop-sender!))
        last-reads (old-reads!)
        pane-count (count (panes))
        pane-set (pane-ids)
        last-logical (:logical-delivery (old-maxima (:ids last-reads)))
        void (not (< last-logical (first (:expected seed))))
        stop-exit (stop-process! old-proc)
        start (now)
        _ (start-poller!)
        old-digest (digest store-path)
        guard (guard-probe @new-package)
        _ (reset! package @new-package)
        proc (start-command! (nexus-command @new-package (:store seed)) @nexus-log)
        serving (serves? proc)
        ready (now)]
    (reset! nexus proc)
    (keep-reads! (str "takeover-" label "-last") last-reads)
    (keep-text! (str "takeover-" label "-window.start") (iso start))
    (keep-text! (str "takeover-" label "-window.ready") (iso ready))
    (swap! takeover assoc :last last-reads :old-digest old-digest :new-proc proc :pane-count pane-count
           :pane-set pane-set :window {:start start :ready ready})
    {:sent sent :void void :stop-exit stop-exit :guard guard :serving serving :start start :ready ready
     :last-logical last-logical :old-digest old-digest}))

(defn switch-step [label silent expect-serving]
  (fn []
    (let [{:keys [sent void stop-exit guard serving start ready last-logical old-digest]} (hand-switch! label silent)
          last-send (some->> sent (map :at) seq (reduce max))]
      {:reply (str "old stopped, exit " stop-exit "; guard after the stop " (pr-str guard) "; new Nexus " (name (or serving :silent)))
       :pass (boolean (and (not void) (not (guard-refuses? guard))
                           (if expect-serving (= serving :serving) (not= serving :serving))))
       :watched (str (when silent (str "sender stopped before the stop, last send " (some-> last-send iso)
                                       ", " (count sent) " sends; "))
                     "last old logical delivery " last-logical (if void ", not below the seed: the seed is void" ", below the seed")
                     "; window.start " (iso start) ", window.ready " (iso ready)
                     "; old store sha256 at the stop " old-digest)})))

(defn rebind!
  "Each line typed into its pane by Herdr, then Bound.<FlowId> awaited there;
  each pane's time from typing to Bound."
  [lines]
  (vec (for [{:keys [pane flow-id address line]} lines]
         (let [started (now)
               typed (herdr "pane" "run" pane line)
               timeout (if (= address codex-secondary) 300000 120000)
               waited (run {:timeout (+ timeout 20000)} "herdr" "--session" session "pane" "wait-output" pane
                           "--match" (str "Bound." flow-id) "--timeout" (str timeout))
               bound (zero? (:exit waited))]
           (when-not bound (keep-text! (str "takeover-rebind-" (str/replace pane ":" "-") ".txt") (pane-text pane)))
           {:pane pane :address address :flow-id flow-id :typed (:exit typed) :bound bound
            :ms (- (now) started) :harness (if (str/starts-with? address "{ Mind") "Codex" "Claude")}))))

(defn rebind-step []
  (fn []
    (let [{:keys [window last pane-count]} @takeover
          lines (rebind-lines (panes) @new-package #(or (str/ends-with? % "Haiku") (str/ends-with? % "Terra")))
          _ (keep-text! "takeover-arm-a-rebind.lines" (str/join "\n" (map #(str (:pane %) " " (:flow-id %) " " (:line %)) lines)))
          bound (rebind! lines)
          readback-start (now)
          readback (flow "Metaflows")
          end (now)
          counts (stop-poller!)
          after (awake-seats (read-datom readback))
          expected (select-keys (:awake last) (map :address lines))
          unbound (changed-seats expected after)
          strangers (vec (sort (for [[a f] after :when (not= f (get (:awake last) a))] a)))
          most-panes (reduce max 0 (map second counts))
          measure (window-measure {:start (:start window) :ready (:ready window) :end end
                                   :pane-ms (map :ms bound) :readback-ms (- end readback-start) :seats night-seats})
          claude (some #(and (:bound %) (= "Claude" (:harness %))) bound)
          codex (some #(and (:bound %) (= "Codex" (:harness %))) bound)
          no-duplicate (and (<= most-panes pane-count) (empty? strangers))]
      (keep-text! "takeover-arm-a-after.rebind.metaflows" readback)
      (keep-text! "takeover-arm-a-window.end" (iso end))
      (keep-text! "takeover-arm-a-window.txt"
                  (str (pr-str measure) "\nper pane " (pr-str bound) "\npane counts " (pr-str counts) "\n"))
      (swap! takeover assoc :rebound bound)
      {:reply (str "readback " readback)
       :pass (boolean (and (seq expected) no-duplicate (empty? unbound) claude (or skip-codex codex)
                           (:extrapolated-s measure)))
       :watched (str "no duplicate seat " (boolean no-duplicate) " (most panes " most-panes " of " pane-count
                     " before; Awake under another FlowId or newly " strangers ")"
                     "; re-bound " (- (count expected) (count unbound)) " of " (count expected) ", not " unbound
                     "; Bound in a Claude pane " (boolean claude) ", in a Codex pane "
                     (if skip-codex "SKIPPED (FLOW_CAPSULE_SKIP_CODEX=1)" (boolean codex))
                     "; window " (pr-str measure) " against the night's 20-minute beat")})))

(defn losses-step []
  (fn []
    (let [{:keys [queued-id]} @takeover
          reads (state-reads)
          primary-queue (get (:queues reads) asleep-primary "")
          carried (notice-queued? (:metaflows reads) asleep-primary queued-id)
          listed (read-datom (:metaflows reads))
          pasts (into {} (for [m (items (body listed))] [(address-text (first (items m))) (vec (items (nth (items m) 2)))]))]
      {:reply (str "new Fable queue " primary-queue)
       :pass (boolean (and (not (str/blank? queued-id)) (not carried)
                           (every? empty? (vals pasts))))
       :watched (str "the old Nexus's queued Notice " (if (str/blank? queued-id) "(none was queued)" queued-id)
                     (if carried " carried" " not carried")
                     "; Past by Address under the new Nexus " (pr-str pasts))})))

(defn ids-step []
  (fn []
    (let [{:keys [seed old-ids sender-pane]} @takeover
          sent (send-to quaternary "Queue" "Soft.Notice.«After the switch. Answer with one word and stop. Tag tk2.»" sender-pane)
          reads (state-reads)
          new-ids (into (issued-ids sent) (:ids reads))
          again (reissued old-ids new-ids)
          low (below-counters new-ids (:expected seed))
          first-logical (some->> new-ids (filter #(str/starts-with? % "logical-delivery-")) sort first)
          not-empty (flow-meta (:counters seed))]
      (keep-reads! "takeover-arm-a-new" reads)
      {:reply (str "Send after the switch: " sent " / Counters on the used store: " not-empty)
       :pass (boolean (and (seq new-ids) (empty? again) (empty? low) (str/includes? not-empty "NotEmpty")))
       :watched (str (count new-ids) " ids the new Nexus issued, the first logical delivery " first-logical
                     " (seeded " (first (:expected seed)) "); reissued " again "; below the seed " low
                     "; " (count old-ids) " ids the old Nexus issued")})))

(defn rollback!
  "The new Nexus stopped, the old one started by its recorded command on its
  untouched store, its readings compared with its last before the stop, the
  queued Notice found by its own id; then the switch back modelled as the
  procedure means it: the declared start of the new Nexus, its guard first,
  refuses while the old Nexus owns the socket, and starts nothing."
  [label]
  (let [{:keys [new-proc old-command old-digest last queued-id]} @takeover
        _ (stop-poller!)
        new-exit (stop-process! new-proc)
        free (guard-probe @old-package)
        untouched (= old-digest (digest store-path))
        _ (reset! package @old-package)
        proc (start-command! old-command @nexus-log)
        serving (serves? proc)
        _ (reset! nexus proc)
        _ (swap! takeover assoc :old-proc proc)
        reads (old-reads!)
        same-metaflows (= (:metaflows reads) (:metaflows last))
        kept (deliveries-preserved? (:deliveries last) (:deliveries reads))
        held-before (notice-queued? (:metaflows last) asleep-primary queued-id)
        held (notice-queued? (:metaflows reads) asleep-primary queued-id)
        guard (guard-probe @new-package)
        started (atom nil)
        back (declared-start guard #(reset! started (start-command! (nexus-command @new-package (:store (:seed @takeover)))
                                                                    @nexus-log)))
        _ (some-> @started stop-process!)
        still (owner-answers? (flow "Metaflows"))]
    (keep-reads! (str "takeover-" label "-rolled-back") reads)
    {:reply (str "new Nexus exit " new-exit "; old restarted " (name (or serving :silent))
                 "; switch-back declared start " (if (= back :refused) "refused by its guard" "started the new Nexus"))
     :pass (boolean (and (not (guard-refuses? free)) untouched (= serving :serving) same-metaflows kept held-before held
                         (= back :refused) (nil? @started) still (p/alive? proc)))
     :watched (str "sockets free after the stop " (not (guard-refuses? free))
                   "; old store " (if untouched "untouched" "changed") " since the stop"
                   "; Metaflows " (if same-metaflows "equal" "unequal") " to the last read before the stop"
                   "; logical deliveries " (count (:deliveries last)) " before the stop, " (count (:deliveries reads))
                   " after, " (if kept "each the same or Delivered, none lost" "NOT all the same or Delivered, or one lost")
                   " (before " (pr-str (:deliveries last)) ", after " (pr-str (:deliveries reads)) ")"
                   "; queued Notice "
                   (if (str/blank? queued-id) "(none was queued)" queued-id)
                   (if held-before " held in the Metaflows queue of " " not in the Metaflows queue of ") asleep-primary
                   " before the stop,"
                   (if held " held there " " missing there ") "after the rollback"
                   "; switch back: the guard reads " (pr-str guard) ", the declared start "
                   (if (= back :refused) "refused, the new Nexus never started" "passed and started the new Nexus")
                   "; old Nexus PID " (.pid (:proc proc)) (if still " answers" " does not answer") " Metaflows")}))

(defn rollback-step [label] (fn [] (rollback! label)))

(defn refused-store-step []
  (fn []
    (let [{:keys [seed]} @takeover
          _ (spit (:store seed) "not a Flow store\n")
          check (reply-text (run {:env (package-env @new-package {})} (str @new-package "/bin/flow-store-check")
                                 (str "{ «" (:store seed) "» }")))]
      (swap! takeover assoc-in [:seed :digest] (digest (:store seed)))
      {:reply (str "flow-store-check -> " check)
       :pass (not (str/starts-with? check "Opens."))
       :watched (str "the seeded store " (:store seed) " overwritten, so the new Nexus must refuse it")})))

(defn duplicate-step []
  (fn []
    (let [{:keys [last pane-count pane-set]} @takeover
          watched-for (select-keys (:awake last) [quaternary tertiary-address])
          seen (watch 90 #(let [after (awake-seats (read-datom (flow "Metaflows")))
                                fresh (vec (remove pane-set (pane-ids)))
                                moved (vec (for [[a f] watched-for :when (and (get after a) (not= f (get after a)))] a))]
                            (when (or (> (count (panes)) pane-count) (seq moved))
                              {:fresh fresh :moved moved :after after})))
          sent (stop-sender!)
          counts (stop-poller!)
          first-panes (into {} (for [[a f] watched-for] [a (pane-of-flow f)]))
          live (every? some? (vals first-panes))]
      (keep-text! "takeover-arm-b-sends.txt" (str/join "\n" (map pr-str sent)))
      (keep-text! "takeover-arm-b-pane-counts.txt" (pr-str counts))
      {:reply (str "sends in the window " (pr-str (vec (filter #(> (:at %) (:start (:window @takeover))) sent))))
       :pass (boolean (and seen (seq (:fresh seen)) live))
       :watched (str (if seen (str "a second seat: new panes " (:fresh seen) ", Awake under a new FlowId " (:moved seen))
                         "no second seat in 90 s")
                     "; the first panes " first-panes (if live " still live" " not all live")
                     "; no Stop exists to remove it: teardown closes it")})))

(def takeover-steps
  [["T0 old Nexus on the capsule's sockets and store, started by a recorded command"
    (fn []
      (reset! package @old-package)
      (configure! @nexus-log (:old (:configuration @takeover)))
      (let [command (nexus-command @old-package store-path)
            proc (start-command! command @nexus-log)
            serving (serves? proc)]
        (reset! nexus proc)
        (swap! takeover assoc :old-command command :old-proc proc)
        (keep-text! "takeover-old-nexus.command" (pr-str command))
        {:reply (str (name (or serving :silent)) ", PID " (.pid (:proc proc)))
         :pass (= serving :serving)
         :watched (str (str/join " " (:argv command)) "; its command kept as takeover-old-nexus.command")}))]

   ["T1 seats Awake, a Codex seat in a long turn, a Notice queued at an asleep Fable, the sender sending"
    (fn []
      (let [seats (concat [secondary quaternary tertiary-address] (when-not skip-codex [codex-quaternary]))
            launched (into {} (for [addr seats]
                                [addr (let [reply (launch addr)] [reply (watch 90 #(awake-flow addr))])]))
            sender-pane (or (some-> (get-in launched [secondary 1]) str pane-of-flow) (sender-pane))
            _ (swap! takeover assoc :sender-pane sender-pane)
            ;; A Send reaches only an Address that has a metaflow, which its
            ;; first launch makes: the Fable takes one turn and, OnDemand,
            ;; falls asleep before the Notice is queued to it.
            primary-launch (launch asleep-primary)
            primary-woke (watch 90 #(awake-flow asleep-primary))
            primary-slept (watch 180 #(= :asleep (:state (current asleep-primary))))
            queued (send-to asleep-primary "Queue" "Soft.Notice.«Held across the switch. Answer with one word and stop. Tag tk1.»" sender-pane)
            queued-id (accepted-id queued)
            _ (swap! takeover assoc :queued-id queued-id)
            _ (start-sender! sender-pane)
            busy (when-not skip-codex
                   (let [reply (flow (str "Launch.{ " codex-secondary " Predecessors.0 «Run the shell command sleep 240, then answer with one word and stop.» }"))]
                     [reply (watch 90 #(awake-flow codex-secondary))]))
            sends (watch 45 #(let [s @(:sent @sender)] (when (>= (count s) 4) s)))
            primary (current asleep-primary)]
        {:reply (str (pr-str launched) " / Fable " primary-launch " / Codex Sol " (if skip-codex "SKIPPED" (pr-str busy))
                     " / queued " queued)
         :pass (boolean (and (every? (comp second val) launched) (or skip-codex (second busy))
                             sender-pane primary-woke primary-slept queued-id
                             (= :asleep (:state primary)) (holds-id? (pr-str (:queue primary)) queued-id)
                             (seq sends) (every? #(str/starts-with? (:reply %) "SendAccepted.") sends)))
         :watched (str "Awake " (pr-str (into {} (for [[a [_ f]] launched] [a f])))
                       (if skip-codex "; Codex seats SKIPPED (FLOW_CAPSULE_SKIP_CODEX=1)" "")
                       "; sender pane " sender-pane "; Fable awake as " primary-woke ", then "
                       (some-> (:state primary) name) " with the Notice " queued-id
                       (if (holds-id? (pr-str (:queue primary)) queued-id) " queued" " not queued")
                       "; " (count sends) " sends by the loop")}))]

   ["T2 snapshot through the old clients"
    (fn []
      (let [reads (old-reads!)]
        (keep-reads! "takeover-T2" reads)
        (swap! takeover assoc :t2 reads)
        {:reply (str "Awake " (pr-str (:awake reads)) " / " (count (:ids reads)) " ids")
         :pass (boolean (and (seq (:awake reads))
                             (notice-queued? (:metaflows reads) asleep-primary (:queued-id @takeover))))
         :watched (str "the queued Notice " (:queued-id @takeover)
                       (if (notice-queued? (:metaflows reads) asleep-primary (:queued-id @takeover)) " held" " not held")
                       " in the Metaflows queue of " asleep-primary "; " (count (:deliveries reads))
                       " logical delivery states read by ObserveSends; Metaflows, Configuration, ReadSendOutcomes and every"
                       " ReadQueueDeliveries kept as takeover-T2.*")}))]

   ["A-T3 seed: Counters at the old maxima plus 10000, a lower one refused, the replay, flow-store-check" (seed-step "arm-a")]
   ["A-T4 activate first: the guard refuses while the old Nexus answers" (guard-step)]
   ["A-T5 sender silent; hand stop of the old, start of the new; the window opens" (switch-step "arm-a" true true)]
   ["A-T7 re-bind through Herdr, trackers first; no duplicate seat; the window measured" (rebind-step)]
   ["A-T9 losses: the queued Notice and Past are not carried" (losses-step)]
   ["A-T8 ids: none the old Nexus issued is reissued; Counters refused on a used store" (ids-step)]
   ["A-T10 rollback after success: the old Nexus by its recorded command; switch back refused" (rollback-step "arm-a")]

   ["R-T3 seed a second store" (seed-step "refusal")]
   ["R-T3b the second store overwritten, so the new Nexus must refuse it" (refused-store-step)]
   ["R-T4 the guard refuses while the old Nexus answers" (guard-step)]
   ["R-T5 hand stop of the old; the new Nexus refuses its store" (switch-step "refusal" false false)]
   ["R-T10 rollback after the refusal: the old Nexus by its recorded command; switch back refused" (rollback-step "refusal")]

   ["B-T3 seed a third store" (seed-step "arm-b")]
   ["B-T4 the sender sending; the guard refuses while the old Nexus answers"
    (let [guard (guard-step)]
      (fn [] (start-sender! (:sender-pane @takeover)) (guard)))]
   ["B-T5 hand stop of the old, start of the new, the sender still sending" (switch-step "arm-b" false true)]
   ["B control: a Wake in the window launches a second seat beside the live one" (duplicate-step)]])

(defn takeover-main [& args]
  (let [[old new & options] args]
    (when-not (and old new (not-any? #(str/starts-with? % "--") [old new]))
      (refuse "usage: flow-capsule --takeover <old flow package> <new flow package> [--log <path>]"))
    (reset! old-package (str (fs/canonicalize old)))
    (reset! new-package (str (fs/canonicalize new)))
    (reset! package-interface {@old-package :query @new-package :invocation})
    (reset! package @old-package)
    (let [log (or (second (drop-while #(not= % "--log") options)) (str state-root "/nexus.log"))
          wall (now)
          version-of #(str/trim (:out (run {} (str % "/bin/flow-nexus") "--version")))
          old-version (version-of @old-package)
          new-version (version-of @new-package)
          root (witness-root (System/getenv))]
      (when-not root
        (refuse "no witness directory: set FLOW_CAPSULE_WITNESS_DIR, or HOME and FLOW_ID (or FLOW_DIRECTORY); nothing started"))
      (reset! witness-dir (str root "/" (witness-name (str "takeover " (last (str/split old-version #"\s+"))
                                                           "-to-" (last (str/split new-version #"\s+")))
                                                      (java.time.Instant/now))))
      (reset! run-log log)
      (println (str "flow-capsule --takeover | old " @old-package " | " old-version " | new " @new-package " | "
                    new-version " | curriculum " curriculum-package))
      (println (str "witnesses | " @witness-dir))
      (try
        (verify-private-handover-places!)
        (start-capsule! log)
        (start-curriculum! log)
        (if skip-codex
          (println "codex | FLOW_CAPSULE_SKIP_CODEX=1: app-server not started, Codex seats skipped")
          (start-codex! log))
        (reset! nexus-log log)
        (swap! takeover assoc :configuration (takeover-configuration (configure-lines)))
        ;; Every step runs: a rollback is owed whatever came before it.
        (doseq [[name f] takeover-steps] (scenario name f))
        (catch Exception e
          (println (str "stopped: " (.getMessage e) " " (pr-str (ex-data e))))))
      (let [clean (teardown!)
            passed (and (seq @results) (every? #(or (:pass %) (:skipped %)) @results))]
        (println (str "wall " (- (now) wall) " ms | " (count (filter :pass @results)) " of " (count @results)
                      " steps passed | witnesses " @witness-dir))
        (System/exit (if (and passed clean) 0 1))))))

(defn -main [& args]
  (when (= "--takeover" (first args))
    (apply takeover-main (rest args)))
  (let [[pkg & options] args]
    (when-not (and pkg (not (str/starts-with? pkg "--")))
      (refuse "usage: flow-capsule <flow-package> [--workspace-only | --handover | --handover-h0-diagnostic] [--keep-going] [--log <path>]"))
    (reset! package (str (fs/canonicalize pkg)))
    (reset! package-interface {@package :invocation})
    (reset! keep-going (boolean (some #{"--keep-going"} options)))
    (let [workspace-only (boolean (some #{"--workspace-only"} options))
          handover-only (boolean (some #{"--handover"} options))
          h0-diagnostic-only (boolean (some #{"--handover-h0-diagnostic"} options))
          selected-scenarios (cond workspace-only (subvec base-scenarios 0 2)
                                   h0-diagnostic-only h0-diagnostic-scenarios
                                   handover-only handover-scenarios
                                   :else scenarios)
          log (or (second (drop-while #(not= % "--log") options)) (str state-root "/nexus.log"))
          wall (System/currentTimeMillis)
          passed (atom true)
          version (str/trim (:out (run {} (str @package "/bin/flow-nexus") "--version")))
          root (witness-root (System/getenv))]
      (when-not root
        (refuse "no witness directory: set FLOW_CAPSULE_WITNESS_DIR, or HOME and FLOW_ID (or FLOW_DIRECTORY); nothing started"))
      (reset! witness-dir (str root "/" (witness-name version (java.time.Instant/now))))
      (reset! run-log log)
      (println (str "flow-capsule " @package " | " version
                    " | curriculum " curriculum-package))
      (println (str "witnesses | " @witness-dir))
      (let [run-capsule
            (fn []
              (try
                (verify-private-handover-places!)
                (start-capsule! log)
                (start-curriculum! log)
                (if skip-codex
                  (println "codex | FLOW_CAPSULE_SKIP_CODEX=1: app-server not started, Codex scenarios skipped")
                  (start-codex! log))
                (configure! log)
                (reset! nexus-log log)
                (start-nexus! log)
                (println (str "configuration | " (flow-meta "Configuration")))
                (loop [[[name f] & more] selected-scenarios]
                  (when name
                    (let [ok (scenario name f)]
                      (when-not ok (reset! passed false))
                      (if (or ok @keep-going) (recur more) (println "stopped after a failure")))))
                (catch Exception e
                  (reset! passed false)
                  (println (str "stopped: " (.getMessage e) " " (pr-str (ex-data e)))))))]
        (let [clean (run-with-teardown! run-capsule
                                        #(when h0-diagnostic-only (finish-h0-diagnostic!))
                                        teardown!)
            contract-complete (seven-line-contract-complete? handover-only h0-diagnostic-only @results)
            diagnostic-capture (and h0-diagnostic-only (h0-diagnostic-capture-complete?))]
        (println (str "wall " (- (System/currentTimeMillis) wall) " ms | "
                      (count (filter :pass @results)) " of " (count @results) " scenarios passed, "
                      (count (filter :skipped @results)) " skipped | seven-line contract complete " contract-complete
                      " | diagnostic capture complete " diagnostic-capture
                      " | witnesses " @witness-dir))
          (System/exit (if (and @passed clean contract-complete) 0 1)))))))

(when (= *file* (System/getProperty "babashka.file"))
  (apply -main *command-line-args*))
