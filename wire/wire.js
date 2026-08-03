// wire.js — the provided glue for pages not using the htmx SSE extension.
// Connects an EventSource and, per named event:
//   1. re-dispatches it on document.body as a DOM CustomEvent
//      "wire:<topic>" (detail = the frame data), so htmx pages trigger
//      with hx-trigger="wire:<topic> from:body" and plain pages use
//      addEventListener;
//   2. optionally swaps the frame data into every element whose
//      data-wire-swap attribute lists the topic (data-wire-into names the
//      insertAdjacentHTML position, "beforeend" by default), and lets
//      htmx process anything swapped in.
// EventSource has no wildcard for named events, so the page names the
// topics it uses — the token already bounds what can arrive.
(function () {
  function connect(url, topics, opts) {
    opts = opts || {};
    var swap = opts.swap !== false;
    var es = new EventSource(url);
    (topics || []).concat(["inbox"]).forEach(function (topic) {
      es.addEventListener(topic, function (e) {
        document.body.dispatchEvent(
          new CustomEvent("wire:" + topic, { detail: e.data, bubbles: true })
        );
        if (!swap) return;
        var sel = '[data-wire-swap~="' + topic + '"]';
        document.querySelectorAll(sel).forEach(function (el) {
          el.insertAdjacentHTML(el.dataset.wireInto || "beforeend", e.data);
          if (window.htmx) window.htmx.process(el);
        });
      });
    });
    return es;
  }
  window.wire = { connect: connect };
})();
