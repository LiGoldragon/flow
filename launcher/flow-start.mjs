#!/usr/bin/env node
// flow-start — start a standard flow from its declaration in
// <workspace>/config/main-flow (model, effort, modules, leave-outs).
//
//   flow-start <Aspect> <topic> <Layer> [-p <predecessor>] [-n] [task ...]
//
// Examples:
//   flow-start Psyche core Secondary -p 68db51
//   flow-start Mind flow Secondary "Review the drain fix."
// -n prints the Start datom without launching.
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
// The start adapter and the launch configuration reader live in the
// workspace's tools; FLOW_START_LIBRARY names that tools directory.
const library = process.env.FLOW_START_LIBRARY;
if (!library) { console.error('flow-start: FLOW_START_LIBRARY is unset'); process.exit(2); }
const {startRequest, submitStart} = await import(path.join(library, 'flow-start-adapter.mjs'));
const {loadLaunchConfig, voiceFromConfig, roleModulesFromConfig} = await import(path.join(library, 'main-flow-mode/launch-config.mjs'));

const root = process.env.FLOW_WORKSPACE;
if (!root) { console.error('flow-start: FLOW_WORKSPACE is unset'); process.exit(2); }
const args = process.argv.slice(2);
let predecessor = null, dry = false;
const rest = [];
for (let i = 0; i < args.length; i++) {
  if (args[i] === '-p') predecessor = args[++i];
  else if (args[i] === '-n') dry = true;
  else rest.push(args[i]);
}
const [aspect, topicArg, layer, ...task] = rest;
if (!aspect || !topicArg || !layer) {
  console.error('usage: flow-start <Aspect> <topic> <Layer> [-p <predecessor>] [-n] [task ...]');
  process.exit(2);
}
const config = loadLaunchConfig(root);
const rows = config.metaflow.metaflows.filter(r => r.aspect === aspect && r.layer === layer && r.topic.toLowerCase() === topicArg.toLowerCase());
if (rows.length !== 1) { console.error(`no single declaration for ${aspect} ${topicArg} ${layer} in ${root}/config/main-flow/metaflow`); process.exit(1); }
const row = rows[0];
const topic = row.topic.toLowerCase();
const voice = voiceFromConfig(config, {aspect, layer, topic: row.topic});
let brief = task.join(' ').trim();
if (!brief) {
  brief = predecessor
    ? `Take the seat from ${predecessor}: read flows/${predecessor}/ (log.md, orders.md, handover.md if present) and continue its open orders.`
    : config.shared['secondary-bootstrap-context'];
} else if (predecessor) {
  brief = `Predecessor: ${predecessor} (flows/${predecessor}/).\n\n${brief}`;
}
const profile = {
  requestId: 'flow-start-' + crypto.randomUUID(),
  system: roleModulesFromConfig(config, row, 'system'),
  user: roleModulesFromConfig(config, row, 'user'),
  aspect, topic, layer, model: voice.model, effort: voice.effort,
  predecessor, remembered: predecessor ? [{flowId: predecessor, depth: 1}] : [],
  session: 'default', leaveOuts: row['leave-outs'],
  brief: `Address.{ ${aspect} ${topic} ${layer} }\n\n${brief}`,
};
if (dry) { console.log(startRequest(profile)); process.exit(0); }
console.error(`Starting { ${aspect} ${topic} ${layer} } on ${voice.model} ${voice.effort}; request ${profile.requestId}`);
const seconds = config.shared['first-response-timeout-seconds'] ?? 900;
const result = submitStart(profile, {timeout: seconds * 1000});
const dir = path.join(root, 'flows', result.flowId);
try {
  fs.mkdirSync(dir, {recursive: true});
  fs.writeFileSync(path.join(dir, 'continuation.json'), JSON.stringify({flowId: result.flowId, predecessor, topic, launchRequestId: profile.requestId, nativeSessionId: result.nativeSessionId, route: result.route, reachability: result.reachability}) + '\n', {flag: 'wx', mode: 0o600});
} catch (e) { if (e.code !== 'EEXIST') console.error('continuation.json not written: ' + e.message); }
console.log(`Started { ${aspect} ${topic} ${layer} ${result.flowId} }${result.route ? ' pane ' + result.route.name : ' (queue only)'}`);
