// https://github.com/mozilla/glam/blob/e6d345d0e61df23549ece416e2d41e800980de41/src/utils/stats.js
(function() {
function sum(vs) { return vs.reduce((a, b) => a + b, 0); }

function ascending(a, b) {
  if (a < b) return -1;
  if (a > b) return 1;
  if (a >= b) return 0;
  return NaN;
}

function byKey(k) {
  return function sortByKey(a, b) {
    if (a[k] < b[k]) return -1;
    if (a[k] > b[k]) return 1;
    if (a[k] >= b[k]) return 0;
    return NaN;
  };
}

function orderedNumbers(values, valueof) {
  return Float64Array.from(values.map(valueof)).sort(ascending);
}

function cumSum(values, valueof = (v) => v) {
  return values.map(valueof).reduce((acc, v, i) => {
    if (i === 0) {
      acc.push(v);
    } else {
      acc.push(acc[i - 1] + v);
    }
    return acc;
  }, []);
}

function nnInterp(x, y, xOut) {
  // returns yOut by mapping x -> xOut, finding the index, then applying to y
  // constant (nearest-neighbors) interpolation with f=1 (always right-sided)
  // find ind in x where xOut values should below, then look at y[ind]
  // assume x & y are pre-sorted and match.
  const xInds = xOut.map((xi) => x.findIndex((xx) => xx > xi));
  const low = Math.min(...x);
  const high = Math.max(...x);
  return xInds.map((i) => {
    // left & right censor extreme values.
    if (i === -1 && xOut[i] <= low) return y[0];
    if (i === -1 && xOut[i] >= high) return y[y.length - 1];
    // otherwise, return the corresponding y.
    return y[i];
  });
}

function nearestBelow(value, neighbors) {
  const below = neighbors.filter((n) => n <= value);
  if (!below.length) return neighbors[neighbors.length - 1];
  return Math.max(...below);
}

weightedQuantile = function (probs = [0.05, 0.25, 0.5, 0.75, 0.95],
  values,
  weights = values.map(() => 1)) {
  // rough port of Hmisc's wtd.quantile function. https://github.com/harrelfe/Hmisc/blob/master/R/wtd.stats.s
  const n = sum(weights);
  // in the case where n === values.length, I believe this is just R-7
  const order = probs.map((p) => 1 + (n - 1) * p);
  const low = order.map((o) => Math.max(Math.floor(o), 1));
  const high = low.map((l) => Math.min(l + 1, n));
  const modOrder = order.map((o) => o % 1);
  // here is where approx comes in handy, but our specific use-case is easy to recreate
  const lowStats = nnInterp(cumSum(weights), values, low);
  const highStats = nnInterp(cumSum(weights), values, high);
  const quantiles = modOrder.map((o, i) => (1 - o) * lowStats[i] + o * highStats[i]);
  return quantiles;
}
}());