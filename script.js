const reducedMotionQuery = window.matchMedia("(prefers-reduced-motion: reduce)");
let reducedMotion = reducedMotionQuery.matches;

document.documentElement.dataset.motion = reducedMotion ? "reduced" : "full";

const revealItems = Array.from(document.querySelectorAll(".reveal"));
const heroCommand = document.getElementById("hero-command");
const heroCards = Array.from(document.querySelectorAll("[data-hero-card]"));
const workflowSteps = Array.from(document.querySelectorAll(".workflow-step"));
const workflowPanels = Array.from(document.querySelectorAll(".workflow-panel"));
const workflowKicker = document.getElementById("workflow-kicker");
const counters = Array.from(document.querySelectorAll(".count-up"));
const incidentSteps = Array.from(document.querySelectorAll("[data-incident-step]"));
const incidentPhaseLabel = document.getElementById("incident-phase-label");

const workflowMeta = {
  understand: "Context loaded",
  decompose: "Task graph generated",
  orchestrate: "Assignments live",
  execute: "Outcome verified"
};

const incidentLabels = [
  "Pulling live sensor data",
  "Dispatching rover inspection",
  "Alerting the technician",
  "Preparing the incident report"
];

function setVisible(entries, observer) {
  entries.forEach((entry) => {
    if (entry.isIntersecting) {
      entry.target.classList.add("is-visible");
      observer.unobserve(entry.target);
    }
  });
}

if (!reducedMotion && "IntersectionObserver" in window) {
  const revealObserver = new IntersectionObserver(setVisible, {
    threshold: 0.18,
    rootMargin: "0px 0px -8% 0px"
  });

  revealItems.forEach((item) => {
    if (!item.classList.contains("is-visible")) {
      revealObserver.observe(item);
    }
  });
} else {
  revealItems.forEach((item) => item.classList.add("is-visible"));
}

function activateWorkflow(stepName) {
  workflowSteps.forEach((step) => {
    const active = step.dataset.step === stepName;
    step.classList.toggle("is-active", active);
    step.setAttribute("aria-selected", active ? "true" : "false");
  });

  workflowPanels.forEach((panel) => {
    const active = panel.dataset.panel === stepName;
    panel.classList.toggle("is-active", active);
    panel.hidden = !active;
  });

  if (workflowKicker) {
    workflowKicker.textContent = workflowMeta[stepName] || "Workflow active";
  }
}

workflowSteps.forEach((step) => {
  step.addEventListener("click", () => activateWorkflow(step.dataset.step || "understand"));
});

if ("IntersectionObserver" in window) {
  const stepObserver = new IntersectionObserver((entries) => {
    const visibleStep = entries
      .filter((entry) => entry.isIntersecting)
      .sort((a, b) => b.intersectionRatio - a.intersectionRatio)[0];

    if (visibleStep) {
      activateWorkflow(visibleStep.target.dataset.step || "understand");
    }
  }, {
    threshold: [0.45, 0.7]
  });

  workflowSteps.forEach((step) => stepObserver.observe(step));
}

function animateCounter(counter) {
  if (counter.dataset.animated === "true") {
    return;
  }

  const target = Number(counter.dataset.count || counter.textContent || "0");
  counter.dataset.animated = "true";

  if (reducedMotion) {
    counter.textContent = String(target);
    return;
  }

  const duration = 1400;
  const startTime = performance.now();

  function tick(now) {
    const progress = Math.min((now - startTime) / duration, 1);
    const eased = 1 - Math.pow(1 - progress, 3);
    counter.textContent = String(Math.round(target * eased));

    if (progress < 1) {
      window.requestAnimationFrame(tick);
    } else {
      counter.textContent = String(target);
    }
  }

  counter.textContent = "0";
  window.requestAnimationFrame(tick);
}

if ("IntersectionObserver" in window) {
  const counterObserver = new IntersectionObserver((entries, observer) => {
    entries.forEach((entry) => {
      if (entry.isIntersecting) {
        animateCounter(entry.target);
        observer.unobserve(entry.target);
      }
    });
  }, { threshold: 0.6 });

  counters.forEach((counter) => counterObserver.observe(counter));
} else {
  counters.forEach((counter) => animateCounter(counter));
}

function cycleHeroConsole() {
  if (!heroCommand || heroCards.length === 0 || reducedMotion) {
    return;
  }

  const commands = (heroCommand.dataset.commands || "")
    .split("|")
    .map((item) => item.trim())
    .filter(Boolean);

  if (commands.length === 0) {
    return;
  }

  let index = 0;

  window.setInterval(() => {
    index = (index + 1) % commands.length;
    heroCommand.textContent = commands[index];
    heroCards.forEach((card, cardIndex) => {
      card.classList.toggle("is-active", cardIndex === index % heroCards.length);
    });
  }, 2600);
}

function cycleIncidentTimeline() {
  if (incidentSteps.length === 0) {
    return;
  }

  let activeIndex = 0;

  function applyState(index) {
    incidentSteps.forEach((step, stepIndex) => {
      const state = step.querySelector(".incident-state");
      const isCurrent = stepIndex === index;
      const isDone = stepIndex < index;

      step.classList.toggle("is-current", isCurrent);

      if (state) {
        if (isCurrent) {
          state.textContent = "Running";
        } else if (isDone) {
          state.textContent = "Done";
        } else {
          state.textContent = "Queued";
        }
      }
    });

    if (incidentPhaseLabel) {
      incidentPhaseLabel.textContent = incidentLabels[index] || incidentLabels[0];
    }
  }

  applyState(activeIndex);

  if (reducedMotion) {
    return;
  }

  window.setInterval(() => {
    activeIndex = (activeIndex + 1) % incidentSteps.length;
    applyState(activeIndex);
  }, 2400);
}

function handleReducedMotionChange(event) {
  reducedMotion = event.matches;
  document.documentElement.dataset.motion = reducedMotion ? "reduced" : "full";
}

if (typeof reducedMotionQuery.addEventListener === "function") {
  reducedMotionQuery.addEventListener("change", handleReducedMotionChange);
} else if (typeof reducedMotionQuery.addListener === "function") {
  reducedMotionQuery.addListener(handleReducedMotionChange);
}

activateWorkflow("understand");
cycleHeroConsole();
cycleIncidentTimeline();
