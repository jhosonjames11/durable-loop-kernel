import assert from 'node:assert/strict';
import test from 'node:test';

import * as core from '@loopgraph/core';
import { createDemoSummary } from '@loopgraph/demo';

test('built workspace packages are executable through their declared exports', () => {
  assert.equal(core.harnessNeutral, true);
  assert.deepEqual(createDemoSummary('operator'), {
    message: 'LoopGraph demo ready for operator',
    harnessNeutral: true,
  });
});
