(() => {
  const starting = document.querySelector("#starting");
  const failed = document.querySelector("#failed");
  const errorMessage = document.querySelector("#error-message");
  const allowedActions = new Set(["retry", "reassign-port", "open-logs", "quit"]);

  window.setDesktopState = ({ kind, message = "" }) => {
    const hasFailed = kind === "failed";
    starting.hidden = hasFailed;
    failed.hidden = !hasFailed;
    if (hasFailed) {
      errorMessage.textContent = message || "请重试或查看日志。";
    }
  };

  document.addEventListener("click", (event) => {
    const button = event.target.closest("button[data-action]");
    if (!button) {
      return;
    }
    const action = button.dataset.action;
    if (allowedActions.has(action)) {
      window.location.href = `cy-kaf-action://${action}`;
    }
  });
})();
