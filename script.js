const modules = {
  "bd-intelligence": {
    title: "Market and Account Intelligence",
    summary: "Build high-quality account intelligence and qualification briefs before the first commercial call.",
    kpis: ["+30% faster account qualification", "Higher lead relevance", "Reduced manual research effort"],
    features: [
      "Auto-build account briefs from internal opportunities and external data.",
      "Suggest buyer roles, likely needs, and risk indicators.",
      "Generate discovery question packs by vertical and product line.",
      "Track qualification confidence to prioritize sales effort."
    ],
    integrations: [
      "CRM opportunity and account objects.",
      "Public and subscribed market intelligence sources.",
      "Sales inbox and meeting notes.",
      "Role-based dashboards for leadership visibility."
    ],
    image: "./assets/screen-bd-intelligence.svg",
    alt: "Market and Account Intelligence interface"
  },
  "opportunity-engine": {
    title: "Opportunity Structuring Engine",
    summary: "Normalize incoming requests, detect missing details, and route best next actions for the team.",
    kpis: ["-35% clarification loops", "More complete briefs", "Faster architect assignment"],
    features: [
      "Ingest brief templates, emails, and attached files into one normalized case.",
      "Find similar historical opportunities and pull known bottlenecks.",
      "Highlight incomplete technical and commercial fields.",
      "Draft a prioritized clarification checklist for customer and internal teams."
    ],
    integrations: [
      "CRM, brief templates, and knowledge repositories.",
      "Historical offer and estimate archives.",
      "Collaboration tools for handoff and approvals.",
      "Audit trail for all system recommendations and edits."
    ],
    image: "./assets/screen-opportunity-structuring.svg",
    alt: "Opportunity Structuring Engine interface"
  },
  "quote-automation": {
    title: "Quote Automation Studio",
    summary: "Turn customer specs, drawings, PDFs, and product images into structured offer drafts at speed.",
    kpis: ["-40% offer prep time", "Lower quote error rates", "Higher throughput per sales engineer"],
    features: [
      "Parse mixed formats: email text, PDFs, pictures, and part codes.",
      "Extract dimensions and technical parameters from drawings.",
      "Auto-compose proposal draft with assumptions and alternatives.",
      "Provide confidence scoring and exception queues for human review."
    ],
    integrations: [
      "ERP product catalog and availability lookup.",
      "Pricing rules and historical offer structures.",
      "Document generation templates (PDF/Word).",
      "Version control for quote revisions."
    ],
    image: "./assets/screen-quote-automation.svg",
    alt: "Quote Automation Studio interface"
  },
  "estimation-memory": {
    title: "Estimation Memory Graph",
    summary: "Capture and re-use estimation logic so future opportunities start from proven knowledge.",
    kpis: ["+consistency in estimates", "Reduced dependence on specific experts", "Faster onboarding"],
    features: [
      "Store assumptions, effort ranges, and rationale from closed deals.",
      "Retrieve reusable estimate fragments by category and complexity.",
      "Compare new opportunities against historical outcomes.",
      "Highlight margin risks before final offer release."
    ],
    integrations: [
      "Offer management and project delivery data.",
      "Finance and margin analytics datasets.",
      "Knowledge graph or vector store for semantic retrieval.",
      "Governance policies for model updates and approvals."
    ],
    image: "./assets/screen-estimation-memory.svg",
    alt: "Estimation Memory Graph interface"
  },
  "contract-docs": {
    title: "Contract and Documentation Workspace",
    summary: "Accelerate contracting and documentation while keeping traceability, compliance, and control.",
    kpis: ["-25% contracting cycle", "Traceable answers", "Stronger compliance readiness"],
    features: [
      "Generate contract draft support from standard clauses and playbooks.",
      "Answer documentation questions with exact source references.",
      "Flag risky deviations from approved commercial terms.",
      "Maintain timeline of approvals, overrides, and final decisions."
    ],
    integrations: [
      "Contract repositories and policy libraries.",
      "Identity and RBAC management systems.",
      "Compliance logging and audit exports.",
      "Workflow tools for legal and operations alignment."
    ],
    image: "./assets/screen-contract-docs.svg",
    alt: "Contract and Documentation Workspace interface"
  }
};

const moduleButtons = Array.from(document.querySelectorAll(".module-card"));
const detailTitle = document.getElementById("detail-title");
const detailSummary = document.getElementById("detail-summary");
const detailKpis = document.getElementById("detail-kpis");
const detailFeatures = document.getElementById("detail-features");
const detailIntegrations = document.getElementById("detail-integrations");
const detailImage = document.getElementById("detail-image");

function renderModule(moduleId) {
  const module = modules[moduleId];
  if (!module || !detailTitle || !detailSummary || !detailKpis || !detailFeatures || !detailIntegrations || !detailImage) {
    return;
  }

  detailTitle.textContent = module.title;
  detailSummary.textContent = module.summary;

  detailKpis.innerHTML = module.kpis
    .map((kpi) => `<span class="kpi-pill">${kpi}</span>`)
    .join("");

  detailFeatures.innerHTML = module.features
    .map((feature) => `<li>${feature}</li>`)
    .join("");

  detailIntegrations.innerHTML = module.integrations
    .map((integration) => `<li>${integration}</li>`)
    .join("");

  detailImage.src = module.image;
  detailImage.alt = module.alt;
}

if (moduleButtons.length > 0) {
  moduleButtons.forEach((button) => {
    button.addEventListener("click", () => {
      moduleButtons.forEach((item) => item.classList.remove("is-active"));
      button.classList.add("is-active");
      renderModule(button.dataset.module);
    });
  });
}

const oppSlider = document.getElementById("opp-count");
const hoursSlider = document.getElementById("hours-per-quote");
const oppValue = document.getElementById("opp-value");
const hoursValue = document.getElementById("hours-value");
const hoursSaved = document.getElementById("hours-saved");

function updateRoi() {
  const opportunities = Number(oppSlider.value);
  const hoursPerQuote = Number(hoursSlider.value);
  const saved = Math.round(opportunities * hoursPerQuote * 0.3);

  oppValue.textContent = opportunities.toString();
  hoursValue.textContent = hoursPerQuote.toString();
  hoursSaved.textContent = saved.toString();
}

if (oppSlider && hoursSlider && oppValue && hoursValue && hoursSaved) {
  oppSlider.addEventListener("input", updateRoi);
  hoursSlider.addEventListener("input", updateRoi);
  updateRoi();
}
