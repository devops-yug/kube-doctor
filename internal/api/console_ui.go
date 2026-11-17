package api

import (
	"net/http"
	"strings"
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

const openShiftPluginManifestJSON = `{
  "name": "kube-doctor-console-plugin",
  "version": "1.35.0",
  "displayName": "Kube-Doctor Autonomous SRE & GitOps Remediation",
  "description": "Autonomous SRE LLM, Active Incidents, 8-Hop Lineage, In-Cluster RemediationRequest Approvals, and GitHub/GitLab OAuth GitOps for OpenShift >= 4.22",
  "dependencies": {
    "@console/pluginAPI": "*"
  },
  "loadScripts": [
    "plugin-entry.js"
  ],
  "registrationMethod": "callback",
  "extensions": [
    {
      "type": "console.page/route",
      "properties": {
        "exact": false,
        "path": [
          "/kube-doctor",
          "/kube-doctor/incidents",
          "/kube-doctor/approvals",
          "/kube-doctor/gitops"
        ],
        "component": {
          "$codeRef": "KubeDoctorDashboard"
        }
      }
    },
    {
      "type": "console.navigation/section",
      "properties": {
        "id": "kube-doctor-nav-section",
        "name": "Kube-Doctor SRE",
        "perspective": "admin",
        "insertBefore": "compute"
      }
    },
    {
      "type": "console.navigation/href",
      "properties": {
        "id": "kube-doctor-incidents-nav",
        "name": "Active Incidents & Lineage",
        "href": "/kube-doctor/incidents",
        "perspective": "admin",
        "section": "kube-doctor-nav-section"
      }
    },
    {
      "type": "console.navigation/href",
      "properties": {
        "id": "kube-doctor-approvals-nav",
        "name": "In-Cluster & GitOps Approvals",
        "href": "/kube-doctor/approvals",
        "perspective": "admin",
        "section": "kube-doctor-nav-section"
      }
    },
    {
      "type": "console.navigation/href",
      "properties": {
        "id": "kube-doctor-gitops-nav",
        "name": "GitHub & GitLab OAuth",
        "href": "/kube-doctor/gitops",
        "perspective": "admin",
        "section": "kube-doctor-nav-section"
      }
    },
    {
      "type": "console.navigation/href",
      "properties": {
        "id": "kube-doctor-observe-nav",
        "name": "Kube-Doctor SRE",
        "href": "/kube-doctor",
        "perspective": "admin",
        "section": "observe"
      }
    }
  ]
}`

const openShiftPluginEntryJS = `/* OpenShift 4.22+ Dynamic Console Plugin Module Federation Entrypoint (kube-doctor-console-plugin@1.35.0) */
(function () {
  var PLUGIN_ID = 'kube-doctor-console-plugin@1.35.0';
  var PROXY_BASE = '/api/proxy/plugin/kube-doctor-console-plugin/backend';
  var ReactRef = typeof window !== 'undefined' ? window.React : null;
  var SDKRef = null;

  function resolveShared(scopeObj) {
    if (!scopeObj) return Promise.resolve(null);
    var versions = Object.keys(scopeObj);
    if (versions.length === 0) return Promise.resolve(null);
    var entry = scopeObj[versions[0]];
    if (!entry || typeof entry.get !== 'function') return Promise.resolve(null);
    return Promise.resolve(entry.get()).then(function (factory) {
      return typeof factory === 'function' ? factory() : factory;
    });
  }

  function getCSRFToken() {
    var match = document.cookie.match(/(?:^|;\s*)csrf-token=([^;]*)/);
    return match ? decodeURIComponent(match[1]) : '';
  }

  function apiFetch(path, options) {
    var opts = Object.assign({ credentials: 'same-origin' }, options || {});
    opts.headers = Object.assign(
      {
        Accept: 'application/json',
        'Content-Type': 'application/json',
        'X-CSRFToken': getCSRFToken()
      },
      opts.headers || {}
    );
    if (SDKRef && typeof SDKRef.consoleFetchJSON === 'function' && (!opts.method || opts.method === 'GET')) {
      return SDKRef.consoleFetchJSON(PROXY_BASE + path);
    }
    return fetch(PROXY_BASE + path, opts).then(function (res) {
      if (!res.ok) {
        return res.text().then(function (t) {
          throw new Error(t || 'HTTP ' + res.status);
        });
      }
      return res.json();
    });
  }

  function createKubeDoctorDashboard(R) {
    var h = R.createElement;

    return function KubeDoctorDashboard() {
      var initialTab = 'incidents';
      if (typeof window !== 'undefined' && window.location && window.location.pathname) {
        if (window.location.pathname.indexOf('/approvals') !== -1) initialTab = 'approvals';
        else if (window.location.pathname.indexOf('/gitops') !== -1) initialTab = 'gitops';
      }

      var tabState = R.useState(initialTab);
      var activeTab = tabState[0];
      var setActiveTab = tabState[1];

      var incState = R.useState([]);
      var incidents = incState[0];
      var setIncidents = incState[1];

      var appState = R.useState([]);
      var approvals = appState[0];
      var setApprovals = appState[1];

      var gitState = R.useState(null);
      var gitopsStatus = gitState[0];
      var setGitopsStatus = gitState[1];

      var loadState = R.useState(false);
      var loading = loadState[0];
      var setLoading = loadState[1];

      var msgState = R.useState('');
      var statusBanner = msgState[0];
      var setStatusBanner = msgState[1];

      var modeState = R.useState('HYBRID');
      var approvalMode = modeState[0];
      var setApprovalMode = modeState[1];

      var inClustState = R.useState(true);
      var inClusterAllowed = inClustState[0];
      var setInClusterAllowed = inClustState[1];

      var ghIdState = R.useState('');
      var ghSecretState = R.useState('');
      var ghRepoState = R.useState('org/openshift-gitops-fleet');
      var ghBranchState = R.useState('main');

      var glUrlState = R.useState('https://gitlab.com');
      var glIdState = R.useState('');
      var glSecretState = R.useState('');
      var glRepoState = R.useState('org/openshift-gitops-fleet');
      var glBranchState = R.useState('main');

      var loadData = R.useCallback(function () {
        setLoading(true);
        Promise.all([
          apiFetch('/v1/incidents?includeResolved=true').catch(function () { return { incidents: [] }; }),
          apiFetch('/v1/approvals').catch(function () { return { approvals: [] }; }),
          apiFetch('/v1/gitops/status').catch(function () { return null; })
        ])
          .then(function (results) {
            var incResp = results[0] || {};
            var appResp = results[1] || {};
            var gitResp = results[2];
            setIncidents(incResp.incidents || []);
            setApprovals(appResp.approvals || []);
            if (gitResp) {
              setGitopsStatus(gitResp);
              setApprovalMode(gitResp.approvalMode || 'HYBRID');
              setInClusterAllowed(gitResp.inClusterAllowed !== false);
              if (gitResp.github) {
                ghIdState[1](gitResp.github.clientId || '');
                ghRepoState[1](gitResp.github.repository || 'org/openshift-gitops-fleet');
                ghBranchState[1](gitResp.github.baseBranch || 'main');
              }
              if (gitResp.gitlab) {
                glUrlState[1](gitResp.gitlab.webUrl || 'https://gitlab.com');
                glIdState[1](gitResp.gitlab.clientId || '');
                glRepoState[1](gitResp.gitlab.repository || 'org/openshift-gitops-fleet');
                glBranchState[1](gitResp.gitlab.baseBranch || 'main');
              }
            }
          })
          .finally(function () {
            setLoading(false);
          });
      }, []);

      R.useEffect(function () {
        loadData();
      }, [loadData]);

      function triggerScan() {
        setStatusBanner('Scanning cluster pods, nodes, PVCs, quotas, and Operator CRDs...');
        apiFetch('/v1/cluster/scan?force=true', { method: 'POST', body: JSON.stringify({ force: true }) })
          .then(function () {
            setStatusBanner('Cluster scan completed.');
            loadData();
          })
          .catch(function (err) {
            setStatusBanner('Scan failed: ' + err.message);
          });
      }

      function approveInCluster(req) {
        setStatusBanner('Approving and executing remediation in-cluster for ' + req.crdName + '...');
        apiFetch('/v1/approvals/' + encodeURIComponent(req.id) + '/approve', {
          method: 'POST',
          body: JSON.stringify({ approvedBy: 'openshift-console-admin', mode: 'IN_CLUSTER' })
        })
          .then(function () {
            setStatusBanner('Approved & executed in-cluster: ' + req.crdName);
            loadData();
          })
          .catch(function (err) {
            setStatusBanner('Approval error: ' + err.message);
          });
      }

      function rejectRequest(req) {
        apiFetch('/v1/approvals/' + encodeURIComponent(req.id) + '/reject', {
          method: 'POST',
          body: JSON.stringify({ approvedBy: 'openshift-console-admin' })
        }).then(function () {
          setStatusBanner('Rejected approval request: ' + req.crdName);
          loadData();
        });
      }

      function createSCMPR(req, provider) {
        setStatusBanner('Creating ' + provider.toUpperCase() + ' Pull/Merge Request for ' + req.crdName + '...');
        apiFetch('/v1/approvals/' + encodeURIComponent(req.id) + '/create-pr', {
          method: 'POST',
          body: JSON.stringify({ provider: provider })
        })
          .then(function (updated) {
            setStatusBanner('Created ' + provider.toUpperCase() + ' PR/MR: ' + (updated.gitOpsPrUrl || ''));
            loadData();
          })
          .catch(function (err) {
            setStatusBanner('SCM PR/MR error: ' + err.message);
          });
      }

      function saveGitOpsConfig(provider) {
        var payload = {
          approvalMode: approvalMode,
          inClusterAllowed: inClusterAllowed
        };
        if (provider === 'github') {
          payload.provider = 'github';
          payload.clientId = ghIdState[0];
          payload.clientSecret = ghSecretState[0];
          payload.repository = ghRepoState[0];
          payload.baseBranch = ghBranchState[0];
        } else if (provider === 'gitlab') {
          payload.provider = 'gitlab';
          payload.webUrl = glUrlState[0];
          payload.clientId = glIdState[0];
          payload.clientSecret = glSecretState[0];
          payload.repository = glRepoState[0];
          payload.baseBranch = glBranchState[0];
        }
        apiFetch('/v1/gitops/config', { method: 'POST', body: JSON.stringify(payload) }).then(function () {
          setStatusBanner('Saved GitOps & OAuth configuration.');
          loadData();
        });
      }

      function connectOAuth(provider) {
        apiFetch('/v1/gitops/oauth/' + provider + '/authorize?redirect_uri=' + encodeURIComponent(window.location.href))
          .then(function (resp) {
            if (resp && resp.authorizeUrl) {
              window.open(resp.authorizeUrl, '_blank', 'noopener,noreferrer');
            }
            return apiFetch('/v1/gitops/oauth/' + provider + '/callback?code=openshift-console-oauth&state=' + encodeURIComponent(resp.state || 'console'));
          })
          .then(function () {
            setStatusBanner('Connected ' + provider.toUpperCase() + ' OAuth App.');
            loadData();
          });
      }

      function disconnectOAuth(provider) {
        apiFetch('/v1/gitops/oauth/' + provider + '/disconnect', { method: 'POST', body: '{}' }).then(function () {
          setStatusBanner('Disconnected ' + provider.toUpperCase() + ' OAuth App.');
          loadData();
        });
      }

      var activeCount = incidents.filter(function (i) { return i.status !== 'RESOLVED'; }).length;
      var pendingCount = approvals.filter(function (a) { return a.status === 'PENDING'; }).length;

      return h(
        'div',
        { className: 'co-m-pane__body', style: { padding: '24px' } },
        h(
          'div',
          { style: { display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '20px' } },
          h(
            'div',
            null,
            h('h1', { className: 'co-m-pane__heading', style: { margin: 0 } }, 'Kube-Doctor Autonomous SRE & GitOps Control Plane'),
            h(
              'p',
              { className: 'text-muted', style: { marginTop: '4px' } },
              'OpenShift 4.22+ / Kubernetes 1.35+ Active Incidents, 8-Hop CR Lineage, In-Cluster Approvals & GitHub/GitLab OAuth'
            )
          ),
          h(
            'div',
            { style: { display: 'flex', gap: '8px' } },
            h(
              'button',
              { className: 'pf-v5-c-button pf-m-primary', onClick: triggerScan, disabled: loading },
              loading ? 'Scanning...' : 'Scan Cluster Now'
            ),
            h(
              'button',
              { className: 'pf-v5-c-button pf-m-secondary', onClick: loadData, disabled: loading },
              'Refresh Telemetry'
            )
          )
        ),
        statusBanner
          ? h(
              'div',
              {
                className: 'pf-v5-c-alert pf-m-info pf-m-inline',
                style: { padding: '10px 16px', marginBottom: '16px', borderLeft: '4px solid #0066cc' }
              },
              statusBanner
            )
          : null,
        h(
          'div',
          { style: { display: 'flex', gap: '10px', borderBottom: '1px solid #d2d2d2', paddingBottom: '12px', marginBottom: '20px' } },
          h(
            'button',
            {
              className: 'pf-v5-c-button ' + (activeTab === 'incidents' ? 'pf-m-primary' : 'pf-m-tertiary'),
              onClick: function () { setActiveTab('incidents'); }
            },
            'Active Incidents & Lineage (' + activeCount + ')'
          ),
          h(
            'button',
            {
              className: 'pf-v5-c-button ' + (activeTab === 'approvals' ? 'pf-m-primary' : 'pf-m-tertiary'),
              onClick: function () { setActiveTab('approvals'); }
            },
            'In-Cluster & GitOps Approvals (' + pendingCount + ')'
          ),
          h(
            'button',
            {
              className: 'pf-v5-c-button ' + (activeTab === 'gitops' ? 'pf-m-primary' : 'pf-m-tertiary'),
              onClick: function () { setActiveTab('gitops'); }
            },
            'GitHub & GitLab OAuth Integration'
          )
        ),
        activeTab === 'incidents'
          ? h(
              'div',
              null,
              incidents.length === 0
                ? h(
                    'div',
                    { className: 'pf-v5-c-card', style: { padding: '20px' } },
                    'No active cluster incidents detected. Click "Scan Cluster Now" to run an on-demand diagnostic sweep.'
                  )
                : incidents.map(function (inc) {
                    var chain = (inc.assessment && inc.assessment.ownershipChain) || [];
                    return h(
                      'div',
                      { key: inc.incidentId, className: 'pf-v5-c-card', style: { padding: '18px', marginBottom: '16px' } },
                      h(
                        'h3',
                        { style: { marginTop: 0 } },
                        '[' + (inc.status || 'ACTIVE') + '] ' +
                          ((inc.assessment && inc.assessment.issueType) || (inc.event && inc.event.reason) || 'Incident') +
                          (inc.assessment ? ' (' + inc.assessment.criticality + ' / ' + inc.assessment.severity + ')' : '')
                      ),
                      h(
                        'p',
                        null,
                        h('strong', null, 'Workload: '),
                        (inc.event && inc.event.namespace) + '/' + ((inc.event && (inc.event.podName || inc.event.ownerName)) || '')
                      ),
                      h('p', null, (inc.assessment && inc.assessment.summary) || (inc.event && inc.event.message)),
                      chain.length > 0
                        ? h(
                            'p',
                            null,
                            h('strong', null, '8-Hop Owner Lineage: '),
                            chain.map(function (l) { return l.kind + '/' + l.name; }).join(' -> ')
                          )
                        : null,
                      inc.effectiveness
                        ? h(
                            'p',
                            null,
                            h('strong', null, 'Effectiveness Verification: '),
                            inc.effectiveness.verificationStatus + ' (' + inc.effectiveness.effectivenessScore + '/100) — ' + inc.effectiveness.message
                          )
                        : null
                    );
                  })
            )
          : null,
        activeTab === 'approvals'
          ? h(
              'div',
              null,
              approvals.length === 0
                ? h(
                    'div',
                    { className: 'pf-v5-c-card', style: { padding: '20px' } },
                    'No pending or historical RemediationRequest CRDs or GitOps Pull/Merge Requests.'
                  )
                : approvals.map(function (req) {
                    return h(
                      'div',
                      { key: req.id, className: 'pf-v5-c-card', style: { padding: '18px', marginBottom: '16px' } },
                      h(
                        'h3',
                        { style: { marginTop: 0 } },
                        '[' + req.status + '] ' + req.crdName + ' — ' + req.issueType + ' (' + req.criticality + ' / ' + req.severity + ')'
                      ),
                      h('p', null, req.reason),
                      h(
                        'p',
                        null,
                        h('strong', null, 'Approval Mode: '),
                        (req.approvalMode || 'HYBRID') + ' | ',
                        h('strong', null, 'In-Cluster Resource Handling: '),
                        (req.inClusterAllowed !== false ? 'Enabled' : 'GitOps Only') + ' | ',
                        h('strong', null, 'GitOps PR/MR: '),
                        req.gitOpsPrUrl
                          ? h('a', { href: req.gitOpsPrUrl, target: '_blank', rel: 'noreferrer' }, req.gitOpsPrUrl)
                          : 'N/A'
                      ),
                      req.action && req.action.patchPayload
                        ? h(
                            'pre',
                            { style: { padding: '12px', background: '#151515', color: '#f0f0f0', borderRadius: '4px', overflowX: 'auto' } },
                            req.action.patchPayload
                          )
                        : null,
                      req.status === 'PENDING'
                        ? h(
                            'div',
                            { style: { display: 'flex', gap: '8px', marginTop: '12px' } },
                            req.inClusterAllowed !== false
                              ? h(
                                  'button',
                                  {
                                    className: 'pf-v5-c-button pf-m-primary',
                                    onClick: function () { approveInCluster(req); }
                                  },
                                  'Approve & Execute In-Cluster'
                                )
                              : null,
                            h(
                              'button',
                              {
                                className: 'pf-v5-c-button pf-m-secondary',
                                onClick: function () { createSCMPR(req, 'github'); }
                              },
                              'Create GitHub PR'
                            ),
                            h(
                              'button',
                              {
                                className: 'pf-v5-c-button pf-m-secondary',
                                onClick: function () { createSCMPR(req, 'gitlab'); }
                              },
                              'Create GitLab MR'
                            ),
                            h(
                              'button',
                              {
                                className: 'pf-v5-c-button pf-m-danger',
                                onClick: function () { rejectRequest(req); }
                              },
                              'Reject'
                            )
                          )
                        : h('p', null, h('strong', null, 'Processed by: '), req.approvedBy || 'cluster-admin')
                    );
                  })
            )
          : null,
        activeTab === 'gitops'
          ? h(
              'div',
              { style: { display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '16px' } },
              h(
                'div',
                { className: 'pf-v5-c-card', style: { padding: '18px', gridColumn: '1 / -1' } },
                h('h3', { style: { marginTop: 0 } }, 'Cluster Remediation Approval Governance Policy'),
                h(
                  'div',
                  { style: { display: 'flex', alignItems: 'center', gap: '16px', flexWrap: 'wrap' } },
                  h(
                    'label',
                    null,
                    'Default Approval Mode: ',
                    h(
                      'select',
                      {
                        value: approvalMode,
                        onChange: function (e) { setApprovalMode(e.target.value); },
                        style: { padding: '6px', marginLeft: '8px' }
                      },
                      h('option', { value: 'HYBRID' }, 'HYBRID (In-Cluster RemediationRequest CRD + GitHub PR / GitLab MR)'),
                      h('option', { value: 'IN_CLUSTER' }, 'IN_CLUSTER (Direct In-Cluster Approval inside OpenShift Console)'),
                      h('option', { value: 'GITHUB_PR' }, 'GITHUB_PR (GitHub OAuth Pull Request Required)'),
                      h('option', { value: 'GITLAB_MR' }, 'GITLAB_MR (GitLab OAuth Merge Request Required)')
                    )
                  ),
                  h(
                    'label',
                    null,
                    h('input', {
                      type: 'checkbox',
                      checked: inClusterAllowed,
                      onChange: function (e) { setInClusterAllowed(e.target.checked); }
                    }),
                    ' Allow Direct In-Cluster Approval when resources are handled inside the cluster'
                  ),
                  h(
                    'button',
                    {
                      className: 'pf-v5-c-button pf-m-primary',
                      onClick: function () { saveGitOpsConfig(); }
                    },
                    'Save Governance Policy'
                  )
                )
              ),
              h(
                'div',
                { className: 'pf-v5-c-card', style: { padding: '18px' } },
                h(
                  'h3',
                  { style: { marginTop: 0 } },
                  'GitHub OAuth App Integration (' +
                    (gitopsStatus && gitopsStatus.github && gitopsStatus.github.connected
                      ? 'Connected: ' + gitopsStatus.github.connectedUser
                      : 'Not Connected') +
                    ')'
                ),
                h('label', { style: { display: 'block', marginTop: '8px' } }, 'GitHub OAuth Client ID'),
                h('input', {
                  value: ghIdState[0],
                  onChange: function (e) { ghIdState[1](e.target.value); },
                  style: { width: '100%', padding: '6px' }
                }),
                h('label', { style: { display: 'block', marginTop: '8px' } }, 'GitHub OAuth Client Secret'),
                h('input', {
                  type: 'password',
                  value: ghSecretState[0],
                  onChange: function (e) { ghSecretState[1](e.target.value); },
                  style: { width: '100%', padding: '6px' }
                }),
                h('label', { style: { display: 'block', marginTop: '8px' } }, 'GitOps Repository (owner/repo)'),
                h('input', {
                  value: ghRepoState[0],
                  onChange: function (e) { ghRepoState[1](e.target.value); },
                  style: { width: '100%', padding: '6px' }
                }),
                h('label', { style: { display: 'block', marginTop: '8px' } }, 'Base Branch'),
                h('input', {
                  value: ghBranchState[0],
                  onChange: function (e) { ghBranchState[1](e.target.value); },
                  style: { width: '100%', padding: '6px' }
                }),
                h(
                  'div',
                  { style: { display: 'flex', gap: '8px', marginTop: '14px' } },
                  h(
                    'button',
                    { className: 'pf-v5-c-button pf-m-secondary', onClick: function () { saveGitOpsConfig('github'); } },
                    'Save Config'
                  ),
                  h(
                    'button',
                    { className: 'pf-v5-c-button pf-m-primary', onClick: function () { connectOAuth('github'); } },
                    'Connect GitHub OAuth'
                  ),
                  h(
                    'button',
                    { className: 'pf-v5-c-button pf-m-danger', onClick: function () { disconnectOAuth('github'); } },
                    'Disconnect'
                  )
                )
              ),
              h(
                'div',
                { className: 'pf-v5-c-card', style: { padding: '18px' } },
                h(
                  'h3',
                  { style: { marginTop: 0 } },
                  'GitLab OAuth App Integration (' +
                    (gitopsStatus && gitopsStatus.gitlab && gitopsStatus.gitlab.connected
                      ? 'Connected: ' + gitopsStatus.gitlab.connectedUser
                      : 'Not Connected') +
                    ')'
                ),
                h('label', { style: { display: 'block', marginTop: '8px' } }, 'GitLab Instance URL'),
                h('input', {
                  value: glUrlState[0],
                  onChange: function (e) { glUrlState[1](e.target.value); },
                  style: { width: '100%', padding: '6px' }
                }),
                h('label', { style: { display: 'block', marginTop: '8px' } }, 'GitLab Application ID'),
                h('input', {
                  value: glIdState[0],
                  onChange: function (e) { glIdState[1](e.target.value); },
                  style: { width: '100%', padding: '6px' }
                }),
                h('label', { style: { display: 'block', marginTop: '8px' } }, 'GitLab Client Secret'),
                h('input', {
                  type: 'password',
                  value: glSecretState[0],
                  onChange: function (e) { glSecretState[1](e.target.value); },
                  style: { width: '100%', padding: '6px' }
                }),
                h('label', { style: { display: 'block', marginTop: '8px' } }, 'Project Path (group/project)'),
                h('input', {
                  value: glRepoState[0],
                  onChange: function (e) { glRepoState[1](e.target.value); },
                  style: { width: '100%', padding: '6px' }
                }),
                h('label', { style: { display: 'block', marginTop: '8px' } }, 'Base Branch'),
                h('input', {
                  value: glBranchState[0],
                  onChange: function (e) { glBranchState[1](e.target.value); },
                  style: { width: '100%', padding: '6px' }
                }),
                h(
                  'div',
                  { style: { display: 'flex', gap: '8px', marginTop: '14px' } },
                  h(
                    'button',
                    { className: 'pf-v5-c-button pf-m-secondary', onClick: function () { saveGitOpsConfig('gitlab'); } },
                    'Save Config'
                  ),
                  h(
                    'button',
                    { className: 'pf-v5-c-button pf-m-primary', onClick: function () { connectOAuth('gitlab'); } },
                    'Connect GitLab OAuth'
                  ),
                  h(
                    'button',
                    { className: 'pf-v5-c-button pf-m-danger', onClick: function () { disconnectOAuth('gitlab'); } },
                    'Disconnect'
                  )
                )
              )
            )
          : null
      );
    };
  }

  var container = {
    init: function (shareScope) {
      return Promise.all([
        resolveShared(shareScope && shareScope.react),
        resolveShared(shareScope && shareScope['@openshift-console/dynamic-plugin-sdk'])
      ]).then(function (resolved) {
        if (resolved[0]) {
          ReactRef = resolved[0].createElement ? resolved[0] : resolved[0].default || resolved[0];
        }
        if (resolved[1]) {
          SDKRef = resolved[1];
        }
      });
    },
    get: function (moduleRequest) {
      var R = ReactRef || (typeof window !== 'undefined' && window.React);
      var Component = createKubeDoctorDashboard(R);
      return Promise.resolve(function () {
        return {
          __esModule: true,
          default: Component,
          KubeDoctorDashboard: Component
        };
      });
    }
  };

  if (typeof window !== 'undefined' && typeof window.loadPluginEntry === 'function') {
    window.loadPluginEntry(PLUGIN_ID, container);
  }
})();
`

func (s *Server) handleConsolePluginUI(w http.ResponseWriter, r *http.Request) {
	if strings.HasSuffix(r.URL.Path, "plugin-manifest.json") {
		s.handleConsolePluginManifest(w, r)
		return
	}
	if strings.HasSuffix(r.URL.Path, "plugin-entry.js") {
		s.handleConsolePluginEntryJS(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(openShiftConsolePluginHTML))
}

func (s *Server) handleConsolePluginManifest(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(openShiftPluginManifestJSON))
}

func (s *Server) handleConsolePluginEntryJS(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/javascript; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(openShiftPluginEntryJS))
}
