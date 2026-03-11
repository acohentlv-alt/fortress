/**
 * Department Page — Drill-down into a specific department
 */

import { getDepartmentJobs, getJobCompanies } from '../api.js';
import { breadcrumb, statusBadge, companyCard, formatDateTime, escapeHtml, renderPagination } from '../components.js';
import { deptName } from '../constants.js';

export async function renderDepartment(container, dept) {
    const deptNameStr = deptName(dept);
    const jobs = await getDepartmentJobs(dept);

    container.innerHTML = `
        ${breadcrumb([
        { label: 'Dashboard', href: '#/' },
        { label: `${dept} — ${deptNameStr}` },
    ])}
        <h1 class="page-title">${dept} — ${escapeHtml(deptNameStr)}</h1>
        <p class="page-subtitle">Jobs ayant trouvé des entreprises dans ce département</p>

        <div id="dept-jobs-container"></div>
    `;

    const jobsContainer = document.getElementById('dept-jobs-container');

    if (!jobs || jobs.length === 0) {
        jobsContainer.innerHTML = `
            <div class="empty-state">
                <div class="empty-state-icon">📂</div>
                <div class="empty-state-text">Aucun job pour ce département</div>
            </div>
        `;
        return;
    }

    // Render each job as an expandable section
    jobsContainer.innerHTML = jobs.map((j, idx) => `
        <div class="card" style="margin-bottom: var(--space-lg)">
            <div style="display:flex; align-items:center; justify-content:space-between; cursor:pointer"
                 onclick="document.getElementById('job-section-${idx}').classList.toggle('hidden-section')">
                <div>
                    <div style="font-size: var(--font-md); font-weight: 600; color: var(--text-primary); margin-bottom: var(--space-xs)">
                        ${escapeHtml(j.query_name)}
                    </div>
                    <div style="font-size: var(--font-sm); color: var(--text-secondary)">
                        ${formatDateTime(j.created_at)} · ${j.companies_in_dept || 0} entreprises dans le ${dept}
                    </div>
                </div>
                <div style="display:flex; gap:var(--space-sm); align-items:center">
                    ${statusBadge(j.status)}
                    <span style="color: var(--text-muted); font-size: 1.2rem">▼</span>
                </div>
            </div>
            <div id="job-section-${idx}" class="hidden-section" style="margin-top: var(--space-xl)">
                <div class="loading"><div class="spinner"></div></div>
            </div>
        </div>
    `).join('');

    // Add CSS for hidden sections
    if (!document.getElementById('hidden-section-style')) {
        const style = document.createElement('style');
        style.id = 'hidden-section-style';
        style.textContent = '.hidden-section { display: none; }';
        document.head.appendChild(style);
    }

    // Load companies for first job automatically
    if (jobs.length > 0) {
        const firstSection = document.getElementById('job-section-0');
        firstSection.classList.remove('hidden-section');
        await loadJobCompanies(jobs[0].query_id, firstSection);
    }

    // Lazy-load on expand
    jobs.forEach((j, idx) => {
        const section = document.getElementById(`job-section-${idx}`);
        const observer = new MutationObserver(() => {
            if (!section.classList.contains('hidden-section') && section.querySelector('.spinner')) {
                loadJobCompanies(j.query_id, section);
            }
        });
        observer.observe(section, { attributes: true, attributeFilter: ['class'] });
    });
}

async function loadJobCompanies(queryId, section, page = 1) {
    const data = await getJobCompanies(queryId, { page, pageSize: 12 });
    if (!data || !data.companies || data.companies.length === 0) {
        section.innerHTML = `
            <div class="empty-state">
                <div class="empty-state-icon">📭</div>
                <div class="empty-state-text">Aucune entreprise trouvée</div>
            </div>
        `;
        return;
    }

    const totalPages = Math.ceil((data.total || 0) / (data.page_size || 12));
    section.innerHTML = `
        <div class="company-grid">
            ${data.companies.map(c => companyCard(c)).join('')}
        </div>
        ${renderPagination(data.page, totalPages, (p) => loadJobCompanies(queryId, section, p))}
    `;
}
