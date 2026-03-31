(function () {
  const DEFAULT_MIN_MERGE = 32;
  const DEFAULT_MIN_GALLOPING = 7;
  const DEFAULT_TMP_STORAGE_LENGTH = 256;
  const BULK_COPY_THRESHOLD = 32;
  const MAX_TIMSORT_STATE_POOL_SIZE = 1;
  const MAX_POOLED_TMP_LENGTH = DEFAULT_TMP_STORAGE_LENGTH * 64;
  const HAS_ARRAY_COPY_WITHIN =
    typeof Array !== "undefined" &&
    Array.prototype &&
    typeof Array.prototype.copyWithin === "function";

  function defaultComparator(a, b) {
    if (a === b) {
      return 0;
    }

    return a < b ? -1 : 1;
  }

  function minRunLength(n) {
    let r = 0;

    while (n >= DEFAULT_MIN_MERGE) {
      r |= n & 1;
      n >>= 1;
    }

    return n + r;
  }

  function reverseRun(array, lo, hi) {
    let left = lo;
    let right = hi - 1;

    while (left < right) {
      const tmp = array[left];
      array[left] = array[right];
      array[right] = tmp;
      left += 1;
      right -= 1;
    }
  }

  function makeAscendingRun(array, lo, hi, compare) {
    let runHi = lo + 1;
    if (runHi === hi) {
      return 1;
    }

    if (compare(array[runHi], array[lo]) < 0) {
      runHi += 1;
      while (runHi < hi && compare(array[runHi], array[runHi - 1]) < 0) {
        runHi += 1;
      }
      reverseRun(array, lo, runHi);
    } else {
      runHi += 1;
      while (runHi < hi && compare(array[runHi], array[runHi - 1]) >= 0) {
        runHi += 1;
      }
    }

    return runHi - lo;
  }

  function binaryInsertionSort(array, lo, hi, start, compare) {
    let nextStart = start;
    if (nextStart === lo) {
      nextStart += 1;
    }

    for (; nextStart < hi; nextStart += 1) {
      const pivot = array[nextStart];
      let left = lo;
      let right = nextStart;

      while (left < right) {
        const mid = (left + right) >>> 1;
        if (compare(pivot, array[mid]) < 0) {
          right = mid;
        } else {
          left = mid + 1;
        }
      }

      let n = nextStart - left;
      switch (n) {
        case 3:
          array[left + 3] = array[left + 2];
        // falls through
        case 2:
          array[left + 2] = array[left + 1];
        // falls through
        case 1:
          array[left + 1] = array[left];
          break;
        default:
          copyWithinRange(array, left + 1, left, n);
      }

      array[left] = pivot;
    }
  }

  const copyWithinRange = HAS_ARRAY_COPY_WITHIN
    ? function copyWithinRangeNative(array, destStart, srcStart, count) {
        if (count <= 0 || destStart === srcStart) {
          return;
        }

        if (count >= BULK_COPY_THRESHOLD) {
          array.copyWithin(destStart, srcStart, srcStart + count);
          return;
        }

        if (destStart < srcStart) {
          for (let i = 0; i < count; i += 1) {
            array[destStart + i] = array[srcStart + i];
          }
          return;
        }

        for (let i = count - 1; i >= 0; i -= 1) {
          array[destStart + i] = array[srcStart + i];
        }
      }
    : function copyWithinRangeFallback(array, destStart, srcStart, count) {
        if (count <= 0 || destStart === srcStart) {
          return;
        }

        if (destStart < srcStart) {
          for (let i = 0; i < count; i += 1) {
            array[destStart + i] = array[srcStart + i];
          }
          return;
        }

        for (let i = count - 1; i >= 0; i -= 1) {
          array[destStart + i] = array[srcStart + i];
        }
      };

  function copyFromBuffer(destArray, destStart, srcArray, srcStart, count) {
    if (count <= 0) {
      return;
    }

    let i = 0;
    const unrolledLimit = count - (count % 4);
    for (; i < unrolledLimit; i += 4) {
      destArray[destStart + i] = srcArray[srcStart + i];
      destArray[destStart + i + 1] = srcArray[srcStart + i + 1];
      destArray[destStart + i + 2] = srcArray[srcStart + i + 2];
      destArray[destStart + i + 3] = srcArray[srcStart + i + 3];
    }
    for (; i < count; i += 1) {
      destArray[destStart + i] = srcArray[srcStart + i];
    }
  }

  function gallopLeft(value, array, start, length, hint, compare) {
    let lastOffset = 0;
    let maxOffset = 0;
    let offset = 1;

    if (compare(value, array[start + hint]) > 0) {
      maxOffset = length - hint;
      while (
        offset < maxOffset &&
        compare(value, array[start + hint + offset]) > 0
      ) {
        lastOffset = offset;
        offset = (offset << 1) + 1;
        if (offset <= 0) {
          offset = maxOffset;
        }
      }

      if (offset > maxOffset) {
        offset = maxOffset;
      }

      lastOffset += hint;
      offset += hint;
    } else {
      maxOffset = hint + 1;
      while (
        offset < maxOffset &&
        compare(value, array[start + hint - offset]) <= 0
      ) {
        lastOffset = offset;
        offset = (offset << 1) + 1;
        if (offset <= 0) {
          offset = maxOffset;
        }
      }

      if (offset > maxOffset) {
        offset = maxOffset;
      }

      const tmp = lastOffset;
      lastOffset = hint - offset;
      offset = hint - tmp;
    }

    lastOffset += 1;
    while (lastOffset < offset) {
      const mid = lastOffset + ((offset - lastOffset) >>> 1);
      if (compare(value, array[start + mid]) > 0) {
        lastOffset = mid + 1;
      } else {
        offset = mid;
      }
    }

    return offset;
  }

  function gallopRight(value, array, start, length, hint, compare) {
    let lastOffset = 0;
    let maxOffset = 0;
    let offset = 1;

    if (compare(value, array[start + hint]) < 0) {
      maxOffset = hint + 1;
      while (
        offset < maxOffset &&
        compare(value, array[start + hint - offset]) < 0
      ) {
        lastOffset = offset;
        offset = (offset << 1) + 1;
        if (offset <= 0) {
          offset = maxOffset;
        }
      }

      if (offset > maxOffset) {
        offset = maxOffset;
      }

      const tmp = lastOffset;
      lastOffset = hint - offset;
      offset = hint - tmp;
    } else {
      maxOffset = length - hint;
      while (
        offset < maxOffset &&
        compare(value, array[start + hint + offset]) >= 0
      ) {
        lastOffset = offset;
        offset = (offset << 1) + 1;
        if (offset <= 0) {
          offset = maxOffset;
        }
      }

      if (offset > maxOffset) {
        offset = maxOffset;
      }

      lastOffset += hint;
      offset += hint;
    }

    lastOffset += 1;
    while (lastOffset < offset) {
      const mid = lastOffset + ((offset - lastOffset) >>> 1);
      if (compare(value, array[start + mid]) < 0) {
        offset = mid;
      } else {
        lastOffset = mid + 1;
      }
    }

    return offset;
  }

  function computeTmpStorageLength(length) {
    let tmpStorageLength =
      length < 2 * DEFAULT_TMP_STORAGE_LENGTH
        ? length >>> 1
        : DEFAULT_TMP_STORAGE_LENGTH;

    if (tmpStorageLength < 1) {
      tmpStorageLength = 1;
    }

    return tmpStorageLength;
  }

  function computeStackLength(length) {
    return length < 120 ? 5 : length < 1542 ? 10 : length < 119151 ? 19 : 40;
  }

  function TimSortState(array, compare) {
    this.array = array;
    this.compare = compare;
    this.minGallop = DEFAULT_MIN_GALLOPING;
    this.length = array.length;
    this.tmpStorageLength = computeTmpStorageLength(this.length);
    this.tmp = new Array(this.tmpStorageLength);
    this.stackLength = computeStackLength(this.length);
    this.runStart = new Array(this.stackLength);
    this.runLength = new Array(this.stackLength);
    this.stackSize = 0;
  }

  const timSortStatePool = [];

  function resetTimSortState(state, array, compare) {
    const length = array.length;
    const tmpStorageLength = computeTmpStorageLength(length);
    const stackLength = computeStackLength(length);

    state.array = array;
    state.compare = compare;
    state.minGallop = DEFAULT_MIN_GALLOPING;
    state.length = length;
    state.tmpStorageLength = tmpStorageLength;
    state.stackLength = stackLength;
    state.stackSize = 0;

    if (state.tmp.length < tmpStorageLength) {
      state.tmp = new Array(tmpStorageLength);
    }

    if (
      state.runStart.length < stackLength ||
      state.runLength.length < stackLength
    ) {
      state.runStart = new Array(stackLength);
      state.runLength = new Array(stackLength);
    }
  }

  function acquireTimSortState(array, compare) {
    const state =
      timSortStatePool.length > 0
        ? timSortStatePool.pop()
        : new TimSortState(array, compare);

    resetTimSortState(state, array, compare);
    return state;
  }

  function releaseTimSortState(state) {
    if (!state) {
      return;
    }

    state.array = null;
    state.compare = null;
    state.stackSize = 0;

    if (state.tmp.length > MAX_POOLED_TMP_LENGTH) {
      return;
    }

    if (timSortStatePool.length < MAX_TIMSORT_STATE_POOL_SIZE) {
      timSortStatePool.push(state);
    }
  }

  TimSortState.prototype.pushRun = function (runStart, runLength) {
    const stackSize = this.stackSize;
    this.runStart[stackSize] = runStart;
    this.runLength[stackSize] = runLength;
    this.stackSize = stackSize + 1;
  };

  TimSortState.prototype.mergeRuns = function () {
    const runLength = this.runLength;

    while (this.stackSize > 1) {
      let n = this.stackSize - 2;

      if (
        (n >= 1 && runLength[n - 1] <= runLength[n] + runLength[n + 1]) ||
        (n >= 2 && runLength[n - 2] <= runLength[n] + runLength[n - 1])
      ) {
        if (runLength[n - 1] < runLength[n + 1]) {
          n -= 1;
        }
      } else if (runLength[n] > runLength[n + 1]) {
        break;
      }

      this.mergeAt(n);
    }
  };

  TimSortState.prototype.forceMergeRuns = function () {
    const runLength = this.runLength;

    while (this.stackSize > 1) {
      let n = this.stackSize - 2;
      if (n > 0 && runLength[n - 1] < runLength[n + 1]) {
        n -= 1;
      }
      this.mergeAt(n);
    }
  };

  TimSortState.prototype.mergeAt = function (index) {
    const compare = this.compare;
    const array = this.array;
    const runStart = this.runStart;
    const runLength = this.runLength;
    let start1 = runStart[index];
    let length1 = runLength[index];
    const start2 = runStart[index + 1];
    let length2 = runLength[index + 1];

    runLength[index] = length1 + length2;
    if (index === this.stackSize - 3) {
      runStart[index + 1] = runStart[index + 2];
      runLength[index + 1] = runLength[index + 2];
    }
    this.stackSize -= 1;

    let k = gallopRight(array[start2], array, start1, length1, 0, compare);
    start1 += k;
    length1 -= k;
    if (length1 === 0) {
      return;
    }

    length2 = gallopLeft(
      array[start1 + length1 - 1],
      array,
      start2,
      length2,
      length2 - 1,
      compare
    );
    if (length2 === 0) {
      return;
    }

    if (length1 <= length2) {
      this.mergeLow(start1, length1, start2, length2);
    } else {
      this.mergeHigh(start1, length1, start2, length2);
    }
  };

  TimSortState.prototype.mergeLow = function (start1, length1, start2, length2) {
    const compare = this.compare;
    const array = this.array;
    let tmp = this.tmp;
    if (length1 > tmp.length) {
      tmp = new Array(length1);
      this.tmp = tmp;
    }
    const defaultMinGallop = DEFAULT_MIN_GALLOPING;

    copyFromBuffer(tmp, 0, array, start1, length1);

    let cursor1 = 0;
    let cursor2 = start2;
    let dest = start1;

    array[dest] = array[cursor2];
    dest += 1;
    cursor2 += 1;
    length2 -= 1;

    if (length2 === 0) {
      copyFromBuffer(array, dest, tmp, cursor1, length1);
      return;
    }

    if (length1 === 1) {
      copyWithinRange(array, dest, cursor2, length2);
      array[dest + length2] = tmp[cursor1];
      return;
    }

    let minGallop = this.minGallop;

    while (true) {
      let count1 = 0;
      let count2 = 0;
      let shouldExit = false;

      do {
        if (compare(array[cursor2], tmp[cursor1]) < 0) {
          array[dest] = array[cursor2];
          dest += 1;
          cursor2 += 1;
          count2 += 1;
          count1 = 0;
          length2 -= 1;
          if (length2 === 0) {
            shouldExit = true;
            break;
          }
        } else {
          array[dest] = tmp[cursor1];
          dest += 1;
          cursor1 += 1;
          count1 += 1;
          count2 = 0;
          length1 -= 1;
          if (length1 === 1) {
            shouldExit = true;
            break;
          }
        }
      } while ((count1 | count2) < minGallop);

      if (shouldExit) {
        break;
      }

      do {
        count1 = gallopRight(array[cursor2], tmp, cursor1, length1, 0, compare);
        if (count1 !== 0) {
          copyFromBuffer(array, dest, tmp, cursor1, count1);
          dest += count1;
          cursor1 += count1;
          length1 -= count1;
          if (length1 <= 1) {
            shouldExit = true;
            break;
          }
        }

        array[dest] = array[cursor2];
        dest += 1;
        cursor2 += 1;
        length2 -= 1;
        if (length2 === 0) {
          shouldExit = true;
          break;
        }

        count2 = gallopLeft(tmp[cursor1], array, cursor2, length2, 0, compare);
        if (count2 !== 0) {
          copyWithinRange(array, dest, cursor2, count2);
          dest += count2;
          cursor2 += count2;
          length2 -= count2;
          if (length2 === 0) {
            shouldExit = true;
            break;
          }
        }

        array[dest] = tmp[cursor1];
        dest += 1;
        cursor1 += 1;
        length1 -= 1;
        if (length1 === 1) {
          shouldExit = true;
          break;
        }

        minGallop -= 1;
      } while (count1 >= defaultMinGallop || count2 >= defaultMinGallop);

      if (shouldExit) {
        break;
      }

      if (minGallop < 0) {
        minGallop = 0;
      }
      minGallop += 2;
    }

    this.minGallop = minGallop < 1 ? 1 : minGallop;

    if (length1 === 1) {
      copyWithinRange(array, dest, cursor2, length2);
      array[dest + length2] = tmp[cursor1];
    } else if (length1 > 1) {
      copyFromBuffer(array, dest, tmp, cursor1, length1);
    }
  };

  TimSortState.prototype.mergeHigh = function (start1, length1, start2, length2) {
    const compare = this.compare;
    const array = this.array;
    let tmp = this.tmp;
    if (length2 > tmp.length) {
      tmp = new Array(length2);
      this.tmp = tmp;
    }
    const defaultMinGallop = DEFAULT_MIN_GALLOPING;

    copyFromBuffer(tmp, 0, array, start2, length2);

    let cursor1 = start1 + length1 - 1;
    let cursor2 = length2 - 1;
    let dest = start2 + length2 - 1;
    let customCursor = 0;
    let customDest = 0;

    array[dest] = array[cursor1];
    dest -= 1;
    cursor1 -= 1;
    length1 -= 1;

    if (length1 === 0) {
      customCursor = dest - (length2 - 1);
      copyFromBuffer(array, customCursor, tmp, 0, length2);
      return;
    }

    if (length2 === 1) {
      dest -= length1;
      cursor1 -= length1;
      customDest = dest + 1;
      customCursor = cursor1 + 1;
      copyWithinRange(array, customDest, customCursor, length1);
      array[dest] = tmp[cursor2];
      return;
    }

    let minGallop = this.minGallop;
    while (true) {
      let count1 = 0;
      let count2 = 0;
      let shouldExit = false;

      do {
        if (compare(tmp[cursor2], array[cursor1]) < 0) {
          array[dest] = array[cursor1];
          dest -= 1;
          cursor1 -= 1;
          count1 += 1;
          count2 = 0;
          length1 -= 1;
          if (length1 === 0) {
            shouldExit = true;
            break;
          }
        } else {
          array[dest] = tmp[cursor2];
          dest -= 1;
          cursor2 -= 1;
          count2 += 1;
          count1 = 0;
          length2 -= 1;
          if (length2 === 1) {
            shouldExit = true;
            break;
          }
        }
      } while ((count1 | count2) < minGallop);

      if (shouldExit) {
        break;
      }

      do {
        count1 =
          length1 -
          gallopRight(tmp[cursor2], array, start1, length1, length1 - 1, compare);
        if (count1 !== 0) {
          dest -= count1;
          cursor1 -= count1;
          length1 -= count1;
          customDest = dest + 1;
          customCursor = cursor1 + 1;
          copyWithinRange(array, customDest, customCursor, count1);
          if (length1 === 0) {
            shouldExit = true;
            break;
          }
        }

        array[dest] = tmp[cursor2];
        dest -= 1;
        cursor2 -= 1;
        length2 -= 1;
        if (length2 === 1) {
          shouldExit = true;
          break;
        }

        count2 =
          length2 -
          gallopLeft(array[cursor1], tmp, 0, length2, length2 - 1, compare);
        if (count2 !== 0) {
          dest -= count2;
          cursor2 -= count2;
          length2 -= count2;
          customDest = dest + 1;
          customCursor = cursor2 + 1;
          copyFromBuffer(array, customDest, tmp, customCursor, count2);
          if (length2 <= 1) {
            shouldExit = true;
            break;
          }
        }

        array[dest] = array[cursor1];
        dest -= 1;
        cursor1 -= 1;
        length1 -= 1;
        if (length1 === 0) {
          shouldExit = true;
          break;
        }

        minGallop -= 1;
      } while (count1 >= defaultMinGallop || count2 >= defaultMinGallop);

      if (shouldExit) {
        break;
      }

      if (minGallop < 0) {
        minGallop = 0;
      }
      minGallop += 2;
    }

    this.minGallop = minGallop < 1 ? 1 : minGallop;

    if (length2 === 1) {
      dest -= length1;
      cursor1 -= length1;
      customDest = dest + 1;
      customCursor = cursor1 + 1;
      copyWithinRange(array, customDest, customCursor, length1);
      array[dest] = tmp[cursor2];
    } else if (length2 > 1) {
      customCursor = dest - (length2 - 1);
      copyFromBuffer(array, customCursor, tmp, 0, length2);
    }
  };

  function timSortInPlace(array, compare) {
    if (!Array.isArray(array)) {
      throw new TypeError("Can only sort arrays");
    }

    const lo = 0;
    const hi = array.length;
    let remaining = hi - lo;
    if (remaining < 2) {
      return array;
    }

    if (remaining < DEFAULT_MIN_MERGE) {
      const runLen = makeAscendingRun(array, lo, hi, compare);
      binaryInsertionSort(array, lo, hi, lo + runLen, compare);
      return array;
    }

    const timSort = acquireTimSortState(array, compare);
    try {
      const minRun = minRunLength(remaining);
      let currentLo = lo;

      do {
        let runLength = makeAscendingRun(array, currentLo, hi, compare);
        if (runLength < minRun) {
          let force = remaining;
          if (force > minRun) {
            force = minRun;
          }

          binaryInsertionSort(
            array,
            currentLo,
            currentLo + force,
            currentLo + runLength,
            compare
          );
          runLength = force;
        }

        timSort.pushRun(currentLo, runLength);
        timSort.mergeRuns();
        remaining -= runLength;
        currentLo += runLength;
      } while (remaining !== 0);

      timSort.forceMergeRuns();
      return array;
    } finally {
      releaseTimSortState(timSort);
    }
  }

  function FastTimSort(array, compareFn) {
    const compare =
      typeof compareFn === "function" ? compareFn : defaultComparator;
    return timSortInPlace(array, compare);
  }

  window.FastTimSort = FastTimSort;
})();
