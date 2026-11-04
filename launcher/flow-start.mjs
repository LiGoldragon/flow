#!/usr/bin/env node
// The former workspace adapter does not have a packaged, typed socket
// invocation contract. A launcher returns when that contract is part of Flow.
console.error('flow-start: unavailable in this package; launch through a typed Flow client invocation');
process.exit(2);
