// The Pi extension Flow loads into every Pi flow it launches, with
// `-e <this file> --flow-hook <command> --flow-hook-events <names>`: it
// runs the flow hook client on Pi's events with the event as Claude Code's
// hook JSON on stdin, so `flow-hook` reports a Pi flow as it reports a
// Claude one. The command is the one Flow writes into a Claude flow's
// settings: the hook client and its configuration, as one shell line. The
// events are the configured ones, by Claude Code's names, comma-separated;
// an event not named is not reported. The hook reads FLOW_ID from the
// environment Pi inherited.
//
//   session_start        -> SessionStart
//   input                -> UserPromptSubmit, the raw text as the prompt
//   tool_execution_end   -> PostToolUse, the tool's name
//   agent_settled        -> Stop, unless aborted: the session file and the
//                           last assistant text; Pi leaves no work running
//                           after a turn, so no background task
//   session_shutdown     -> SessionEnd; a shutdown that replaces the session
//                           (new, resume, fork, reload) is a clear, since Pi
//                           goes on in the session the next session_start
//                           names
//
// Pi 1.6.0's extension API, read in its docs/extensions.md. A failed or
// absent hook never stops Pi: the extension waits for the hook to exit and
// goes on whatever it answered.

const { spawn } = require("node:child_process");

const FLAG = "flow-hook";
const EVENTS_FLAG = "flow-hook-events";

// The shutdown reasons after which Pi goes on in another session.
const REPLACING = new Set(["new", "resume", "fork", "reload"]);

function assistantText(message) {
  if (!message || message.role !== "assistant") return undefined;
  if (typeof message.content === "string") return message.content;
  if (!Array.isArray(message.content)) return undefined;
  return message.content
    .filter((part) => part && part.type === "text")
    .map((part) => part.text)
    .join("");
}

module.exports = function flowHook(pi) {
  pi.registerFlag(FLAG, {
    description: "The flow hook command Flow reports this session to",
    type: "string",
  });
  pi.registerFlag(EVENTS_FLAG, {
    description: "The native events the flow hook is run on, comma-separated",
    type: "string",
  });

  let lastAssistant;

  function report(ctx, input) {
    const command = pi.getFlag(FLAG);
    if (typeof command !== "string" || command === "") return undefined;
    const events = pi.getFlag(EVENTS_FLAG);
    if (typeof events !== "string") return undefined;
    if (!events.split(",").includes(input.hook_event_name)) return undefined;
    const session = ctx.sessionManager;
    const payload = { session_id: session.getSessionId(), ...input };
    return new Promise((resolve) => {
      const child = spawn(command, {
        shell: true,
        stdio: ["pipe", "ignore", "inherit"],
      });
      child.on("error", () => resolve());
      child.on("close", () => resolve());
      child.stdin.on("error", () => {});
      child.stdin.end(JSON.stringify(payload));
    });
  }

  pi.on("session_start", (_event, ctx) => {
    lastAssistant = undefined;
    return report(ctx, { hook_event_name: "SessionStart" });
  });

  pi.on("input", (event, ctx) =>
    report(ctx, { hook_event_name: "UserPromptSubmit", prompt: event.text }),
  );

  pi.on("tool_execution_end", (event, ctx) =>
    report(ctx, { hook_event_name: "PostToolUse", tool_name: event.toolName }),
  );

  pi.on("message_end", (event) => {
    const text = assistantText(event.message);
    if (text !== undefined) lastAssistant = text;
  });

  pi.on("agent_settled", (event, ctx) => {
    if (event.aborted) return undefined;
    const input = {
      hook_event_name: "Stop",
      background_tasks: [],
    };
    const file = ctx.sessionManager.getSessionFile();
    if (file) input.transcript_path = file;
    if (lastAssistant !== undefined) input.last_assistant_message = lastAssistant;
    return report(ctx, input);
  });

  pi.on("session_shutdown", (event, ctx) =>
    report(ctx, {
      hook_event_name: "SessionEnd",
      reason: REPLACING.has(event.reason) ? "clear" : event.reason,
    }),
  );
};
