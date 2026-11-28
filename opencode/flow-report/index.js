// flow-report: the OpenCode 2.x server plugin a Flow-launched OpenCode
// flow runs with. It is OpenCode's side of Flow's hook: each harness event
// becomes one call of the hook client, `flow-hook <configuration>`, with the
// event on stdin in the hook client's input form, and the client sends the
// Report. The plugin holds no path and no socket: the hook client and its
// configuration datom are this plugin's options, written by Flow into the
// flow's own OpenCode configuration; FLOW_ID reaches the client from the
// server's environment, the pane's.
//
//   first event of a session          -> SessionStart     { session_id }
//   session.prompt hook               -> UserPromptSubmit { session_id prompt_id prompt }
//   tool.execute.after hook           -> PostToolUse      { session_id tool_name }
//   session.execution.succeeded,
//   .failed or .interrupted           -> Stop             { session_id last_assistant_message background_tasks }
//   session.deleted                   -> SessionEnd       { session_id reason }
//
// OpenCode loads the plugin after the first session's session.created, so
// a session is started by the first event that names it. A turn ends with
// the background jobs still running named, so a turn that leaves work in
// flight is not read as rest.
import { spawn } from "node:child_process"

export default {
  id: "flow-report",
  async setup(context) {
    const options = context.options ?? {}
    const hook = options.hook
    const configuration = options.configuration
    if (!hook || !configuration) return
    const started = new Set()
    const lastText = new Map()
    const jobs = new Map()

    const call = (input) =>
      new Promise((resolve) => {
        const child = spawn(hook, [configuration], { stdio: ["pipe", "ignore", "inherit"] })
        child.on("error", resolve)
        child.on("close", resolve)
        child.stdin.end(JSON.stringify(input))
      })

    const start = async (session) => {
      if (!session || started.has(session)) return
      started.add(session)
      await call({ hook_event_name: "SessionStart", session_id: session })
    }

    const backgroundTasks = (session) =>
      [...jobs.values()]
        .filter((job) => job.session === session)
        .map((job) => ({ id: job.id, type: "local_bash", status: "running", description: job.description }))

    context.session.hook("prompt", async (input) => {
      await start(input.sessionID)
      await call({
        hook_event_name: "UserPromptSubmit",
        session_id: input.sessionID,
        prompt_id: input.messageID,
        prompt: input.prompt?.text ?? "",
      })
    })

    context.tool.hook("execute.after", async (input) => {
      await start(input.sessionID)
      await call({ hook_event_name: "PostToolUse", session_id: input.sessionID, tool_name: input.tool })
    })

    ;(async () => {
      for await (const event of context.event.subscribe()) {
        const data = event.data ?? {}
        const session = data.sessionID ?? event.durable?.aggregateID
        switch (event.type) {
          case "session.text.ended":
            lastText.set(session, data.text ?? "")
            break
          case "experimental.job.updated":
            if (data.status === "running") jobs.set(data.id, { id: data.id, session, description: data.title ?? data.id })
            else jobs.delete(data.id)
            break
          case "session.execution.succeeded":
          case "session.execution.failed":
          case "session.execution.interrupted":
            await start(session)
            await call({
              hook_event_name: "Stop",
              session_id: session,
              last_assistant_message: lastText.get(session) ?? "",
              background_tasks: backgroundTasks(session),
            })
            break
          case "session.deleted":
            await call({ hook_event_name: "SessionEnd", session_id: session, reason: "other" })
            started.delete(session)
            break
          default:
            if (session && event.type.startsWith("session.")) await start(session)
        }
      }
    })()
  },
}
