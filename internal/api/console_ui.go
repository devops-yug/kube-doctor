package api

import (
	"net/http"
)

const openShiftConsolePluginHTML = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Kube-Doctor SRE — OpenShift 4.22+ Console Plugin</title>
  <style>
    :root {
      --pf-bg: #0f1214;
      --pf-surface: #1b1d21;
      --pf-border: #2e343d;
      --pf-text: #f0f0f0;
      --pf-muted: #9aa5b1;
      --pf-red: #ee0000;
      --pf-blue: #0066cc;
      --pf-green: #3e8635;
      --pf-gold: #f0ab00;
    }
    body { margin: 0; font-family: "RedHatDisplay", "RedHatText", -apple-system, BlinkMacSystemFont, sans-serif; background: var(--pf-bg); color: var(--pf-text); }
    header { background: #151515; border-bottom: 2px solid var(--pf-red); padding: 14px 24px; display: flex; justify-content: space-between; align-items: center; }
    header h1 { margin: 0; font-size: 18px; display: flex; align-items: center; gap: 10px; }
    .badge { padding: 3px 8px; border-radius: 4px; font-size: 12px; font-weight: 600; }
    .badge-p0 { background: #a30000; color: #fff; }
    .badge-p1 { background: #ec7a08; color: #fff; }
    .badge-ok { background: var(--pf-green); color: #fff; }
    .badge-info { background: var(--pf-blue); color: #fff; }
    .tabs { display: flex; gap: 4px; padding: 0 24px; background: #151515; border-bottom: 1px solid var(--pf-border); }
    .tab { padding: 12px 18px; cursor: pointer; color: var(--pf-muted); border-bottom: 3px solid transparent; font-weight: 600; }
    .tab.active { color: #fff; border-bottom-color: var(--pf-blue); }
    .container { padding: 24px; max-width: 1400px; margin: 0 auto; }
    .card { background: var(--pf-surface); border: 1px solid var(--pf-border); border-radius: 6px; padding: 18px; margin-bottom: 16px; }
    .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(360px, 1fr)); gap: 16px; }
    button { background: var(--pf-blue); color: #fff; border: none; padding: 8px 14px; border-radius: 4px; cursor: pointer; font-weight: 600; margin-right: 6px; }
    button.success { background: var(--pf-green); }
    button.danger { background: #c9190b; }
    button.secondary { background: #3c3f42; }
    input, select { width: 100%; box-sizing: border-box; padding: 8px; margin: 6px 0 12px; background: #0f1214; border: 1px solid var(--pf-border); color: #fff; border-radius: 4px; }
    pre { background: #0b0d0e; padding: 10px; border-radius: 4px; overflow-x: auto; font-size: 12px; border: 1px solid var(--pf-border); }
    .hidden { display: none; }
  </style>
</head>
<body>
  <header>
    <h1><span class="badge badge-p0">OpenShift 4.22+ / K8s 1.35+</span> Kube-Doctor Autonomous SRE Console Plugin</h1>
    <div>
      <button onclick="triggerScan()">Scan Cluster Now</button>
      <button class="secondary" onclick="refreshAll()">Refresh Telemetry</button>
    </div>
  </header>
  <div class="tabs">
    <div id="tab-btn-incidents" class="tab active" onclick="showTab('incidents')">Active Incidents & Lineage</div>
    <div id="tab-btn-approvals" class="tab" onclick="showTab('approvals')">In-Cluster & GitOps Approvals</div>
    <div id="tab-btn-gitops" class="tab" onclick="showTab('gitops')">GitHub & GitLab OAuth Integration</div>
  </div>
  <div class="container">
    <div id="tab-incidents">
      <div id="incidents-list">Loading active cluster incidents...</div>
    </div>
    <div id="tab-approvals" class="hidden">
      <div id="approvals-list">Loading pending RemediationRequest CRDs and GitOps PRs...</div>
    </div>
    <div id="tab-gitops" class="hidden">
      <div class="card">
        <h3>Cluster Remediation Approval Governance Mode</h3>
        <label>Default Approval Mode</label>
        <select id="gov-mode">
          <option value="HYBRID">HYBRID (In-Cluster RemediationRequest CRD + GitHub PR / GitLab MR)</option>
          <option value="IN_CLUSTER">IN_CLUSTER (Direct In-Cluster Approval inside OpenShift Console)</option>
          <option value="GITHUB_PR">GITHUB_PR (GitHub OAuth Pull Request Required)</option>
          <option value="GITLAB_MR">GITLAB_MR (GitLab OAuth Merge Request Required)</option>
        </select>
        <button onclick="saveGovernanceMode()">Save Governance Mode</button>
      </div>
      <div class="grid">
        <div class="card">
          <h3>GitHub OAuth App Integration <span id="gh-status" class="badge badge-info">Checking</span></h3>
          <label>GitHub OAuth Client ID</label><input id="gh-client-id" placeholder="Iv1.8a61f9b3..." />
          <label>GitHub OAuth Client Secret</label><input id="gh-client-secret" type="password" placeholder="••••••••" />
          <label>GitOps Repository (owner/repo)</label><input id="gh-repo" value="org/openshift-gitops-fleet" />
          <label>Base Branch</label><input id="gh-branch" value="main" />
          <button onclick="saveProvider('github')">Save Config</button>
          <button class="success" onclick="connectOAuth('github')">Connect GitHub OAuth</button>
          <button class="danger" onclick="disconnectOAuth('github')">Disconnect</button>
        </div>
        <div class="card">
          <h3>GitLab OAuth App Integration <span id="gl-status" class="badge badge-info">Checking</span></h3>
          <label>GitLab Instance URL</label><input id="gl-url" value="https://gitlab.com" />
          <label>GitLab Application ID</label><input id="gl-client-id" placeholder="application-id..." />
          <label>GitLab Client Secret</label><input id="gl-client-secret" type="password" placeholder="••••••••" />
          <label>GitLab Project Path (group/project)</label><input id="gl-repo" value="org/openshift-gitops-fleet" />
          <label>Base Branch</label><input id="gl-branch" value="main" />
          <button onclick="saveProvider('gitlab')">Save Config</button>
          <button class="success" onclick="connectOAuth('gitlab')">Connect GitLab OAuth</button>
          <button class="danger" onclick="disconnectOAuth('gitlab')">Disconnect</button>
        </div>
      </div>
    </div>
  </div>
  <script>
    function showTab(name) {
      ['incidents', 'approvals', 'gitops'].forEach(t => {
        document.getElementById('tab-' + t).classList.toggle('hidden', t !== name);
        document.getElementById('tab-btn-' + t).classList.toggle('active', t === name);
      });
    }
    async function triggerScan() {
      await fetch('/v1/cluster/scan?force=true', { method: 'POST' });
      await refreshAll();
    }
    async function approveInCluster(id) {
      await fetch('/v1/approvals/' + encodeURIComponent(id) + '/approve', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ approvedBy: 'openshift-console-admin', mode: 'IN_CLUSTER' })
      });
      await refreshAll();
    }
    async function rejectApproval(id) {
      await fetch('/v1/approvals/' + encodeURIComponent(id) + '/reject', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ approvedBy: 'openshift-console-admin' })
      });
      await refreshAll();
    }
    async function createSCMPR(id, provider) {
      await fetch('/v1/approvals/' + encodeURIComponent(id) + '/create-pr', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ provider })
      });
      await refreshAll();
    }
    async function saveGovernanceMode() {
      const approvalMode = document.getElementById('gov-mode').value;
      await fetch('/v1/gitops/config', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ approvalMode, inClusterAllowed: true })
      });
      await refreshAll();
    }
    async function saveProvider(provider) {
      const prefix = provider === 'github' ? 'gh' : 'gl';
      const payload = {
        provider,
        clientId: document.getElementById(prefix + '-client-id').value,
        clientSecret: document.getElementById(prefix + '-client-secret').value,
        repository: document.getElementById(prefix + '-repo').value,
        baseBranch: document.getElementById(prefix + '-branch').value,
      };
      if (provider === 'gitlab') payload.webUrl = document.getElementById('gl-url').value;
      await fetch('/v1/gitops/config', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
      });
      await refreshAll();
    }
    async function connectOAuth(provider) {
      await fetch('/v1/gitops/oauth/' + provider + '/callback?code=sim-console-oauth&state=console');
      await refreshAll();
    }
    async function disconnectOAuth(provider) {
      await fetch('/v1/gitops/oauth/' + provider + '/disconnect', { method: 'POST' });
      await refreshAll();
    }
    async function refreshAll() {
      const [incRes, appRes, gitRes] = await Promise.all([
        fetch('/v1/incidents?includeResolved=true').then(r => r.json()),
        fetch('/v1/approvals').then(r => r.json()),
        fetch('/v1/gitops/status').then(r => r.json())
      ]);
      const incs = incRes.incidents || [];
      document.getElementById('incidents-list').innerHTML = incs.length === 0
        ? '<div class="card">No active cluster incidents detected.</div>'
        : incs.map(i => '<div class="card"><h4><span class="badge badge-p1">' + (i.status || 'ACTIVE') + '</span> '
          + (i.assessment ? i.assessment.issueType + ' (' + i.assessment.criticality + '/' + i.assessment.severity + ')' : i.event.reason)
          + ' — ' + (i.event.namespace || 'cluster') + '/' + (i.event.podName || i.event.ownerName || '') + '</h4>'
          + '<p>' + (i.assessment ? i.assessment.summary : i.event.message) + '</p></div>').join('');

      const apps = appRes.approvals || [];
      document.getElementById('approvals-list').innerHTML = apps.length === 0
        ? '<div class="card">No pending or historical approval requests.</div>'
        : apps.map(a => '<div class="card"><h4><span class="badge badge-p0">' + a.status + '</span> '
          + a.crdName + ' (' + a.issueType + ' — ' + a.criticality + '/' + a.severity + ')</h4>'
          + '<p>' + a.reason + '</p>'
          + '<p><strong>GitOps PR/MR:</strong> <a style="color:#4da3ff" href="' + a.gitOpsPrUrl + '" target="_blank">' + a.gitOpsPrUrl + '</a> (' + (a.gitProvider || 'in-cluster') + ')</p>'
          + '<pre>' + a.action.patchPayload + '</pre>'
          + (a.status === 'PENDING' ? (
              '<button class="success" onclick="approveInCluster(\'' + a.id + '\')">Approve & Execute In-Cluster</button>'
              + '<button onclick="createSCMPR(\'' + a.id + '\', \'github\')">Create GitHub PR</button>'
              + '<button onclick="createSCMPR(\'' + a.id + '\', \'gitlab\')">Create GitLab MR</button>'
              + '<button class="danger" onclick="rejectApproval(\'' + a.id + '\')">Reject</button>'
            ) : '<span class="badge badge-ok">Processed by ' + (a.approvedBy || 'admin') + '</span>')
          + '</div>').join('');

      if (gitRes.github) {
        document.getElementById('gh-status').textContent = gitRes.github.connected ? 'Connected (' + gitRes.github.connectedUser + ')' : 'Disconnected';
        document.getElementById('gh-status').className = 'badge ' + (gitRes.github.connected ? 'badge-ok' : 'badge-info');
      }
      if (gitRes.gitlab) {
        document.getElementById('gl-status').textContent = gitRes.gitlab.connected ? 'Connected (' + gitRes.gitlab.connectedUser + ')' : 'Disconnected';
        document.getElementById('gl-status').className = 'badge ' + (gitRes.gitlab.connected ? 'badge-ok' : 'badge-info');
      }
    }
    refreshAll();
  </script>
</body>
</html>`

func (s *Server) handleConsolePluginUI(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(openShiftConsolePluginHTML))
}
