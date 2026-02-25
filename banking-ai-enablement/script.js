const useCases = {
  screening: {
    title: "Onboarding Screening",
    summary:
      "Aggregate data from LexisNexis, WorldCheck, and open web search into a standardized screening output with source traceability.",
    scope: [
      "Name-based search intake mask and identity normalization.",
      "Parallel source checks with result aggregation in one report.",
      "Match scoring and routing for analyst review."
    ],
    controls: [
      "PII pseudonymization for sensitive flows.",
      "Evidence links and timestamped source references.",
      "Human approval gates before final case decision."
    ],
    impact: [
      "Faster onboarding throughput.",
      "More consistent screening quality.",
      "Traceable decisions for internal and external audits."
    ]
  },
  kyc: {
    title: "KYC and Due Diligence",
    summary:
      "Assist analysts in collecting, structuring, and validating due diligence evidence across fragmented internal and external systems.",
    scope: [
      "Document ingestion and extraction for KYC packets.",
      "Risk signal enrichment from sanctioned data providers.",
      "Case summary generation for committee reviews."
    ],
    controls: [
      "Role-based access and segregation of duties.",
      "Structured model and prompt version tracking.",
      "Content safety filters for regulated environments."
    ],
    impact: [
      "Reduced analyst rework in recurring checks.",
      "Faster cycle from intake to risk recommendation.",
      "Higher consistency in KYC decision packages."
    ]
  },
  ops: {
    title: "Operations Copilot",
    summary:
      "Provide front-to-back operations teams with guided answers, next actions, and workflow acceleration for recurring service tasks.",
    scope: [
      "Natural language query layer on policy and process knowledge.",
      "Action suggestions for common service operations.",
      "Template-based response drafts and handoff support."
    ],
    controls: [
      "Permission-aware retrieval from approved knowledge bases.",
      "Usage telemetry for performance and risk oversight.",
      "Escalation workflows for uncertain model responses."
    ],
    impact: [
      "Lower handling time for repetitive requests.",
      "Improved service consistency across teams.",
      "Faster onboarding of new operations staff."
    ]
  },
  compliance: {
    title: "Compliance Reporting",
    summary:
      "Automate evidence collection and report drafting for periodic compliance obligations with full auditability.",
    scope: [
      "Automated pull of required controls evidence.",
      "Pre-formatted report assembly and commentary drafts.",
      "Exception identification and owner assignment."
    ],
    controls: [
      "Immutable audit trail for report generation history.",
      "Policy-aligned templates approved by compliance function.",
      "Versioned outputs with reviewer sign-off."
    ],
    impact: [
      "Shorter reporting cycles.",
      "Reduced manual collation effort.",
      "Higher confidence in regulator-facing responses."
    ]
  }
};

const tabs = Array.from(document.querySelectorAll(".tab"));
const caseTitle = document.getElementById("case-title");
const caseSummary = document.getElementById("case-summary");
const caseScope = document.getElementById("case-scope");
const caseControls = document.getElementById("case-controls");
const caseImpact = document.getElementById("case-impact");

function renderList(node, items) {
  if (!node) return;
  node.innerHTML = items.map((item) => `<li>${item}</li>`).join("");
}

function renderUseCase(id) {
  const data = useCases[id];
  if (!data || !caseTitle || !caseSummary || !caseScope || !caseControls || !caseImpact) return;

  caseTitle.textContent = data.title;
  caseSummary.textContent = data.summary;
  renderList(caseScope, data.scope);
  renderList(caseControls, data.controls);
  renderList(caseImpact, data.impact);
}

if (tabs.length > 0) {
  tabs.forEach((tab) => {
    tab.addEventListener("click", () => {
      tabs.forEach((item) => item.classList.remove("is-active"));
      tab.classList.add("is-active");
      renderUseCase(tab.dataset.case);
    });
  });
}

const revealNodes = Array.from(document.querySelectorAll(".reveal"));
if ("IntersectionObserver" in window && revealNodes.length > 0) {
  const observer = new IntersectionObserver(
    (entries) => {
      entries.forEach((entry) => {
        if (entry.isIntersecting) {
          entry.target.classList.add("is-visible");
          observer.unobserve(entry.target);
        }
      });
    },
    { threshold: 0.12 }
  );

  revealNodes.forEach((node) => observer.observe(node));
} else {
  revealNodes.forEach((node) => node.classList.add("is-visible"));
}
