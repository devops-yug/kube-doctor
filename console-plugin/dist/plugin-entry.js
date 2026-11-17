/* OpenShift 4.22+ Dynamic Console Plugin Module Federation Entrypoint (kube-doctor-console-plugin@1.35.0) */
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
