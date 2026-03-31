(function () {
  const DEFAULT_MAX_RENDER_ROWS = 15;
  const DEFAULT_ROW_HEIGHT = 34;
  const MIN_THUMB_HEIGHT = 26;

  function toPositiveInt(value, fallback) {
    const parsed = Number.parseInt(value, 10);
    if (!Number.isFinite(parsed) || parsed <= 0) {
      return fallback;
    }

    return parsed;
  }

  function clamp(value, minValue, maxValue) {
    if (value < minValue) {
      return minValue;
    }

    if (value > maxValue) {
      return maxValue;
    }

    return value;
  }

  function createVirtualTableRenderer(options) {
    const renderOptions = options || {};
    const containerEl = renderOptions.containerEl;
    const bodyEl = renderOptions.bodyEl;
    const columnCount = toPositiveInt(renderOptions.columnCount, 1);

    if (!containerEl || !bodyEl) {
      throw new Error("Virtual renderer requires container and body elements.");
    }

    let rowHeight = toPositiveInt(renderOptions.rowHeight, DEFAULT_ROW_HEIGHT);
    let maxRenderRows = toPositiveInt(
      renderOptions.maxRenderRows,
      DEFAULT_MAX_RENDER_ROWS
    );
    let currentRowCount = 0;
    let getCellValue = null;
    let startRowFloat = 0;
    let startRow = 0;
    let frameRequested = false;
    let destroyed = false;
    let draggingThumb = false;
    let dragStartClientY = 0;
    let dragStartThumbTop = 0;
    let hasMeasuredRowHeight = false;
    let mountedRowCount = 0;

    const rowPool = [];
    const scrollbarTrackEl = document.createElement("div");
    scrollbarTrackEl.className = "virtualScrollbarTrack";
    const scrollbarThumbEl = document.createElement("div");
    scrollbarThumbEl.className = "virtualScrollbarThumb";
    scrollbarTrackEl.appendChild(scrollbarThumbEl);
    containerEl.appendChild(scrollbarTrackEl);
    containerEl.classList.add("withVirtualScrollbar");

    function getVisibleRows() {
      if (currentRowCount <= 0) {
        return 0;
      }

      return Math.max(1, Math.min(maxRenderRows, currentRowCount));
    }

    function getMaxStartRow() {
      return Math.max(0, currentRowCount - getVisibleRows());
    }

    function getTrackMetrics() {
      const trackHeight = scrollbarTrackEl.clientHeight;
      const visibleRows = getVisibleRows();

      if (trackHeight <= 0 || currentRowCount <= 0 || visibleRows <= 0) {
        return {
          trackHeight,
          thumbHeight: trackHeight,
          maxThumbTop: 0,
          maxStartRow: getMaxStartRow(),
        };
      }

      const ratio = visibleRows / currentRowCount;
      const thumbHeight = clamp(
        Math.round(trackHeight * ratio),
        MIN_THUMB_HEIGHT,
        trackHeight
      );
      const maxThumbTop = Math.max(0, trackHeight - thumbHeight);

      return {
        trackHeight,
        thumbHeight,
        maxThumbTop,
        maxStartRow: getMaxStartRow(),
      };
    }

    function syncThumbFromState() {
      const metrics = getTrackMetrics();
      scrollbarThumbEl.style.height = `${metrics.thumbHeight}px`;

      if (metrics.maxStartRow <= 0 || metrics.maxThumbTop <= 0) {
        scrollbarThumbEl.style.transform = "translateY(0px)";
        scrollbarThumbEl.classList.add("disabled");
        return;
      }

      scrollbarThumbEl.classList.remove("disabled");
      const ratio = startRow / metrics.maxStartRow;
      const thumbTop = ratio * metrics.maxThumbTop;
      scrollbarThumbEl.style.transform = `translateY(${thumbTop}px)`;
    }

    function syncTrackHorizontalPosition() {
      const offsetX = containerEl.scrollLeft || 0;
      scrollbarTrackEl.style.transform = `translateX(${offsetX}px)`;
    }

    function createRowElement() {
      const tr = document.createElement("tr");
      const cells = new Array(columnCount);
      for (let i = 0; i < columnCount; i += 1) {
        const td = document.createElement("td");
        tr.appendChild(td);
        cells[i] = td;
      }

      return { tr, cells };
    }

    function ensureRowPool(size) {
      while (rowPool.length < size) {
        rowPool.push(createRowElement());
      }
    }

    function ensureMountedRows(visibleRows) {
      let needsRemount = bodyEl.children.length !== visibleRows;

      if (!needsRemount) {
        for (let i = 0; i < visibleRows; i += 1) {
          if (bodyEl.children[i] !== rowPool[i].tr) {
            needsRemount = true;
            break;
          }
        }
      }

      if (!needsRemount) {
        mountedRowCount = visibleRows;
        return;
      }

      const fragment = document.createDocumentFragment();
      for (let i = 0; i < visibleRows; i += 1) {
        fragment.appendChild(rowPool[i].tr);
      }
      bodyEl.replaceChildren(fragment);
      mountedRowCount = visibleRows;
    }

    function setStartRow(nextStart, keepSmooth) {
      const maxStart = getMaxStartRow();
      const clampedFloat = clamp(nextStart, 0, maxStart);
      startRowFloat = clampedFloat;
      const nextInt = keepSmooth ? Math.floor(clampedFloat) : Math.round(clampedFloat);
      const normalizedStart = clamp(nextInt, 0, maxStart);
      if (normalizedStart !== startRow) {
        startRow = normalizedStart;
        requestUpdate();
      } else {
        syncThumbFromState();
      }
    }

    function renderVisibleRows() {
      frameRequested = false;

      if (destroyed) {
        return;
      }

      if (currentRowCount <= 0 || typeof getCellValue !== "function") {
        bodyEl.replaceChildren();
        mountedRowCount = 0;
        syncThumbFromState();
        return;
      }

      const visibleRows = getVisibleRows();
      const maxStart = getMaxStartRow();
      if (startRow > maxStart) {
        startRow = maxStart;
      }

      ensureRowPool(visibleRows);
      ensureMountedRows(visibleRows);
      let renderedCount = 0;

      for (let localRow = 0; localRow < visibleRows; localRow += 1) {
        const actualRowIndex = startRow + localRow;
        if (actualRowIndex >= currentRowCount) {
          break;
        }

        const rowView = rowPool[localRow];
        for (let col = 0; col < columnCount; col += 1) {
          const value = getCellValue(actualRowIndex, col);
          const nextText =
            value === undefined || value === null ? "" : String(value);
          if (rowView.cells[col].textContent !== nextText) {
            rowView.cells[col].textContent = nextText;
          }
        }
        renderedCount += 1;
      }
      if (renderedCount !== mountedRowCount) {
        ensureMountedRows(renderedCount);
      }

      if (!hasMeasuredRowHeight && renderedCount > 0) {
        const measured = rowPool[0].tr.offsetHeight;
        if (measured > 0) {
          hasMeasuredRowHeight = true;
          rowHeight = measured;
        }
      }

      syncThumbFromState();
    }

    function requestUpdate() {
      if (frameRequested || destroyed) {
        return;
      }

      frameRequested = true;
      requestAnimationFrame(renderVisibleRows);
    }

    function onWheel(event) {
      if (destroyed) {
        return;
      }

      if (Math.abs(event.deltaY) < Math.abs(event.deltaX) || event.shiftKey) {
        return;
      }

      event.preventDefault();
      const deltaRows = event.deltaY / rowHeight;
      setStartRow(startRowFloat + deltaRows, true);
    }

    function onContainerScroll() {
      syncTrackHorizontalPosition();
    }

    function onThumbMouseDown(event) {
      if (scrollbarThumbEl.classList.contains("disabled")) {
        return;
      }

      draggingThumb = true;
      dragStartClientY = event.clientY;
      const transform = scrollbarThumbEl.style.transform || "translateY(0px)";
      const match = /translateY\(([-\d.]+)px\)/.exec(transform);
      dragStartThumbTop = match ? Number(match[1]) : 0;
      document.body.classList.add("virtualScrollbarDragging");
      event.preventDefault();
    }

    function onTrackMouseDown(event) {
      if (event.target === scrollbarThumbEl) {
        return;
      }

      const metrics = getTrackMetrics();
      if (metrics.maxStartRow <= 0 || metrics.maxThumbTop <= 0) {
        return;
      }

      const rect = scrollbarTrackEl.getBoundingClientRect();
      const clickedTop = event.clientY - rect.top - metrics.thumbHeight / 2;
      const clampedTop = clamp(clickedTop, 0, metrics.maxThumbTop);
      const ratio = clampedTop / metrics.maxThumbTop;
      setStartRow(ratio * metrics.maxStartRow, false);
      event.preventDefault();
    }

    function onDocumentMouseMove(event) {
      if (!draggingThumb || destroyed) {
        return;
      }

      const metrics = getTrackMetrics();
      if (metrics.maxStartRow <= 0 || metrics.maxThumbTop <= 0) {
        return;
      }

      const deltaY = event.clientY - dragStartClientY;
      const nextThumbTop = clamp(
        dragStartThumbTop + deltaY,
        0,
        metrics.maxThumbTop
      );
      const ratio = nextThumbTop / metrics.maxThumbTop;
      setStartRow(ratio * metrics.maxStartRow, false);
      event.preventDefault();
    }

    function stopDragging() {
      if (!draggingThumb) {
        return;
      }

      draggingThumb = false;
      document.body.classList.remove("virtualScrollbarDragging");
    }

    containerEl.style.overflowY = "hidden";
    containerEl.addEventListener("wheel", onWheel, { passive: false });
    containerEl.addEventListener("scroll", onContainerScroll, { passive: true });
    scrollbarThumbEl.addEventListener("mousedown", onThumbMouseDown);
    scrollbarTrackEl.addEventListener("mousedown", onTrackMouseDown);
    document.addEventListener("mousemove", onDocumentMouseMove);
    document.addEventListener("mouseup", stopDragging);

    let resizeObserver = null;
    if (typeof ResizeObserver !== "undefined") {
      resizeObserver = new ResizeObserver(() => {
        syncTrackHorizontalPosition();
        requestUpdate();
      });
      resizeObserver.observe(containerEl);
    }

    syncTrackHorizontalPosition();

    return {
      render(nextState) {
        const state = nextState || {};
        const nextRowCount = Number(state.rowCount);
        currentRowCount = Number.isFinite(nextRowCount)
          ? Math.max(0, Math.floor(nextRowCount))
          : 0;
        getCellValue =
          typeof state.getCellValue === "function" ? state.getCellValue : null;
        if (state.keepScroll !== true) {
          startRowFloat = 0;
          startRow = 0;
        } else {
          setStartRow(startRowFloat, false);
        }

        requestUpdate();
      },
      clear() {
        currentRowCount = 0;
        getCellValue = null;
        startRowFloat = 0;
        startRow = 0;
        mountedRowCount = 0;
        bodyEl.replaceChildren();
        syncThumbFromState();
      },
      setMaxRenderRows(nextMaxRenderRows) {
        maxRenderRows = toPositiveInt(nextMaxRenderRows, DEFAULT_MAX_RENDER_ROWS);
        setStartRow(startRowFloat, false);
      },
      destroy() {
        destroyed = true;
        containerEl.removeEventListener("wheel", onWheel);
        containerEl.removeEventListener("scroll", onContainerScroll);
        scrollbarThumbEl.removeEventListener("mousedown", onThumbMouseDown);
        scrollbarTrackEl.removeEventListener("mousedown", onTrackMouseDown);
        document.removeEventListener("mousemove", onDocumentMouseMove);
        document.removeEventListener("mouseup", stopDragging);
        if (resizeObserver) {
          resizeObserver.disconnect();
        }
        if (scrollbarTrackEl.parentNode) {
          scrollbarTrackEl.parentNode.removeChild(scrollbarTrackEl);
        }
      },
    };
  }

  window.fastTableRendering = {
    createVirtualTableRenderer,
  };
})();
