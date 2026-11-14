import * as React from 'react';
import { consoleFetchJSON, k8sPatch } from '@openshift-console/dynamic-plugin-sdk';

const PROXY_BASE = '/api/proxy/plugin/kube-doctor-console-plugin/backend';

export interface OwnerChainLink {
  depth: number;
  apiVersion: string;
  kind: string;
  name: string;
  namespace?: string;
  phase?: string;
}

export interface RemediationAction {
  actionId: string;
  targetApiVersion: string;
  targetKind: string;
  targetNamespace: string;
  targetName: string;
  operation: string;
  policy: string;
  patchPayload: string;
  description: string;
  status: string;
  approvalId?: string;
  gitOpsPrUrl?: string;
}

export interface IncidentRecord {
  incidentId: string;
  clusterId?: string;
  status: 'ACTIVE' | 'RESOLVED';
  source: string;
  detectedAt: string;
  event: {
    namespace: string;
    podName?: string;
    ownerKind?: string;
    ownerName?: string;
    reason: string;
    message: string;
  };
  assessment?: {
    issueType: string;
    criticality: string;
    severity: string;
    summary: string;
    rootCauseAnalysis: string;
    ownershipChain?: OwnerChainLink[];
    plannedActions?: RemediationAction[];
  };
  effectiveness?: {
    verificationStatus: string;
    effectivenessScore: number;
    message: string;
  };
}

export interface ApprovalRequest {
  id: string;
  crdName: string;
  namespace: string;
  issueType: string;
  criticality: string;
  severity: string;
  reason: string;
  approvalMode: string;
  gitProvider: string;
  inClusterAllowed: boolean;
  action: RemediationAction;
  followUpActions?: RemediationAction[];
  gitOpsPrNumber?: number;
  gitOpsPrUrl?: string;
  gitOpsBranch?: string;
  status: 'PENDING' | 'APPROVED' | 'REJECTED';
  approvedBy?: string;
}

export interface OAuthProviderStatus {
  provider: 'github' | 'gitlab';
  clientId: string;
  hasSecret: boolean;
  webUrl: string;
  apiUrl: string;
  repository: string;
  baseBranch: string;
  connected: boolean;
  connectedUser?: string;
}

export interface GitOpsIntegrationStatus {
  approvalMode: 'IN_CLUSTER' | 'GITHUB_PR' | 'GITLAB_MR' | 'HYBRID';
  inClusterAllowed: boolean;
  github: OAuthProviderStatus;
  gitlab: OAuthProviderStatus;
}

const RemediationRequestModel = {
  apiGroup: 'kubedoctor.io',
  apiVersion: 'v1alpha1',
  kind: 'RemediationRequest',
  plural: 'remediationrequests',
  namespaced: true,
};

export const KubeDoctorDashboard: React.FC = () => {
  const [activeTab, setActiveTab] = React.useState<'incidents' | 'approvals' | 'gitops'>('incidents');
  const [incidents, setIncidents] = React.useState<IncidentRecord[]>([]);
  const [approvals, setApprovals] = React.useState<ApprovalRequest[]>([]);
  const [gitopsStatus, setGitopsStatus] = React.useState<GitOpsIntegrationStatus | null>(null);
  const [loading, setLoading] = React.useState<boolean>(false);

  // GitHub & GitLab OAuth form state
  const [approvalMode, setApprovalMode] = React.useState<string>('HYBRID');
  const [inClusterAllowed, setInClusterAllowed] = React.useState<boolean>(true);
  const [ghClientId, setGhClientId] = React.useState<string>('');
  const [ghClientSecret, setGhClientSecret] = React.useState<string>('');
  const [ghRepo, setGhRepo] = React.useState<string>('org/openshift-gitops-fleet');
  const [ghBranch, setGhBranch] = React.useState<string>('main');

  const [glUrl, setGlUrl] = React.useState<string>('https://gitlab.com');
  const [glClientId, setGlClientId] = React.useState<string>('');
  const [glClientSecret, setGlClientSecret] = React.useState<string>('');
  const [glRepo, setGlRepo] = React.useState<string>('org/openshift-gitops-fleet');
  const [glBranch, setGlBranch] = React.useState<string>('main');

  const loadData = React.useCallback(async () => {
    setLoading(true);
    try {
      const [incResp, appResp, gitResp] = await Promise.all([
        consoleFetchJSON(`${PROXY_BASE}/v1/incidents?includeResolved=true`),
        consoleFetchJSON(`${PROXY_BASE}/v1/approvals`),
        consoleFetchJSON(`${PROXY_BASE}/v1/gitops/status`),
      ]);
      setIncidents(incResp?.incidents || []);
      setApprovals(appResp?.approvals || []);
      if (gitResp) {
        setGitopsStatus(gitResp);
        setApprovalMode(gitResp.approvalMode || 'HYBRID');
        setInClusterAllowed(gitResp.inClusterAllowed !== false);
        if (gitResp.github) {
          setGhClientId(gitResp.github.clientId || '');
          setGhRepo(gitResp.github.repository || 'org/openshift-gitops-fleet');
          setGhBranch(gitResp.github.baseBranch || 'main');
        }
        if (gitResp.gitlab) {
          setGlUrl(gitResp.gitlab.webUrl || 'https://gitlab.com');
          setGlClientId(gitResp.gitlab.clientId || '');
          setGlRepo(gitResp.gitlab.repository || 'org/openshift-gitops-fleet');
          setGlBranch(gitResp.gitlab.baseBranch || 'main');
        }
      }
    } finally {
      setLoading(false);
    }
  }, []);

  React.useEffect(() => {
    loadData();
  }, [loadData]);

  const triggerClusterScan = async () => {
    await consoleFetchJSON.post(`${PROXY_BASE}/v1/cluster/scan?force=true`, { force: true });
    await loadData();
  };

  // In-Cluster Approval: patches the RemediationRequest CRD in-cluster AND invokes the Kube-Doctor approval API
  const handleApproveInCluster = async (req: ApprovalRequest) => {
    try {
      await k8sPatch({
        model: RemediationRequestModel,
        resource: { metadata: { name: req.crdName, namespace: req.namespace } },
        data: [{ op: 'replace', path: '/spec/approved', value: true }],
      });
    } catch {
      // Fallback if CRD is managed via Kube-Doctor API server directly
    }
    await consoleFetchJSON.post(`${PROXY_BASE}/v1/approvals/${req.id}/approve`, {
      approvedBy: 'openshift-console-admin',
    });
    await loadData();
  };

  const handleReject = async (req: ApprovalRequest) => {
    await consoleFetchJSON.post(`${PROXY_BASE}/v1/approvals/${req.id}/reject`, {
      approvedBy: 'openshift-console-admin',
    });
    await loadData();
  };

  const handleCreateSCMPR = async (req: ApprovalRequest, provider: 'github' | 'gitlab') => {
    await consoleFetchJSON.post(`${PROXY_BASE}/v1/approvals/${req.id}/create-pr`, { provider });
    await loadData();
  };

  const handleSaveGitOpsConfig = async (provider?: 'github' | 'gitlab') => {
    const body: Record<string, unknown> = {
      approvalMode,
      inClusterAllowed,
    };
    if (provider === 'github') {
      body.provider = 'github';
      body.clientId = ghClientId;
      body.clientSecret = ghClientSecret;
      body.repository = ghRepo;
      body.baseBranch = ghBranch;
    } else if (provider === 'gitlab') {
      body.provider = 'gitlab';
      body.webUrl = glUrl;
      body.clientId = glClientId;
      body.clientSecret = glClientSecret;
      body.repository = glRepo;
      body.baseBranch = glBranch;
    }
    await consoleFetchJSON.post(`${PROXY_BASE}/v1/gitops/config`, body);
    await loadData();
  };

  const handleStartOAuth = async (provider: 'github' | 'gitlab') => {
    const authResp = await consoleFetchJSON(
      `${PROXY_BASE}/v1/gitops/oauth/${provider}/authorize?redirect_uri=${encodeURIComponent(window.location.href)}`,
    );
    if (authResp?.authorizeUrl) {
      window.open(authResp.authorizeUrl, '_blank', 'noopener,noreferrer');
    }
  };

  const handleDisconnectOAuth = async (provider: 'github' | 'gitlab') => {
    await consoleFetchJSON.post(`${PROXY_BASE}/v1/gitops/oauth/${provider}/disconnect`, {});
    await loadData();
  };

  return (
    <div className="co-m-pane__body" style={{ padding: '24px' }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '20px' }}>
        <div>
          <h1 className="co-m-pane__heading">Kube-Doctor Autonomous SRE &amp; GitOps Control Plane</h1>
          <p className="text-muted">
            Kubernetes &gt;= 1.35 &amp; Red Hat OpenShift &gt;= 4.22 Active Incidents, In-Cluster Approvals &amp; GitHub/GitLab OAuth GitOps
          </p>
        </div>
        <div>
          <button className="pf-v5-c-button pf-m-primary" onClick={triggerClusterScan} disabled={loading}>
            Scan Cluster Now
          </button>{' '}
          <button className="pf-v5-c-button pf-m-secondary" onClick={loadData} disabled={loading}>
            Refresh
          </button>
        </div>
      </div>

      <div style={{ display: 'flex', gap: '12px', marginBottom: '20px', borderBottom: '1px solid #444', paddingBottom: '10px' }}>
        <button
          className={`pf-v5-c-button ${activeTab === 'incidents' ? 'pf-m-primary' : 'pf-m-tertiary'}`}
          onClick={() => setActiveTab('incidents')}
        >
          Active Incidents ({incidents.filter((i) => i.status !== 'RESOLVED').length})
        </button>
        <button
          className={`pf-v5-c-button ${activeTab === 'approvals' ? 'pf-m-primary' : 'pf-m-tertiary'}`}
          onClick={() => setActiveTab('approvals')}
        >
          In-Cluster &amp; GitOps Approvals ({approvals.filter((a) => a.status === 'PENDING').length})
        </button>
        <button
          className={`pf-v5-c-button ${activeTab === 'gitops' ? 'pf-m-primary' : 'pf-m-tertiary'}`}
          onClick={() => setActiveTab('gitops')}
        >
          GitHub &amp; GitLab OAuth Integration
        </button>
      </div>

      {activeTab === 'incidents' && (
        <div>
          {incidents.length === 0 ? (
            <div className="pf-v5-c-card" style={{ padding: '16px' }}>
              No active incidents detected in the cluster.
            </div>
          ) : (
            incidents.map((inc) => (
              <div key={inc.incidentId} className="pf-v5-c-card" style={{ padding: '16px', marginBottom: '16px' }}>
                <h3>
                  [{inc.status}] {inc.assessment?.issueType || inc.event.reason} —{' '}
                  {inc.assessment ? `${inc.assessment.criticality} / ${inc.assessment.severity}` : ''}
                </h3>
                <p>
                  <strong>Target:</strong> {inc.event.namespace}/{inc.event.podName || inc.event.ownerName}
                </p>
                <p>{inc.assessment?.summary || inc.event.message}</p>
                {inc.assessment?.ownershipChain && inc.assessment.ownershipChain.length > 0 && (
                  <p>
                    <strong>8-Hop Owner Lineage:</strong>{' '}
                    {inc.assessment.ownershipChain.map((l) => `${l.kind}/${l.name}`).join(' -> ')}
                  </p>
                )}
                {inc.effectiveness && (
                  <p>
                    <strong>Post-Remediation Effectiveness:</strong> {inc.effectiveness.verificationStatus} (Score:{' '}
                    {inc.effectiveness.effectivenessScore}/100) — {inc.effectiveness.message}
                  </p>
                )}
              </div>
            ))
          )}
        </div>
      )}

      {activeTab === 'approvals' && (
        <div>
          {approvals.length === 0 ? (
            <div className="pf-v5-c-card" style={{ padding: '16px' }}>
              No pending RemediationRequest CRDs or GitOps Pull/Merge Requests.
            </div>
          ) : (
            approvals.map((req) => (
              <div key={req.id} className="pf-v5-c-card" style={{ padding: '16px', marginBottom: '16px' }}>
                <h3>
                  [{req.status}] {req.crdName} — {req.issueType} ({req.criticality} / {req.severity})
                </h3>
                <p>{req.reason}</p>
                <p>
                  <strong>Approval Mode:</strong> {req.approvalMode || 'HYBRID'} |{' '}
                  <strong>In-Cluster Resource Handling:</strong> {req.inClusterAllowed ? 'Enabled' : 'GitOps Only'} |{' '}
                  <strong>GitOps PR/MR ({req.gitProvider || 'in-cluster'}):</strong>{' '}
                  <a href={req.gitOpsPrUrl} target="_blank" rel="noreferrer">
                    {req.gitOpsPrUrl}
                  </a>
                </p>
                <pre style={{ padding: '10px', background: '#111', color: '#eee' }}>{req.action.patchPayload}</pre>
                {req.status === 'PENDING' && (
                  <div style={{ display: 'flex', gap: '8px', marginTop: '12px' }}>
                    {req.inClusterAllowed && (
                      <button className="pf-v5-c-button pf-m-primary" onClick={() => handleApproveInCluster(req)}>
                        Approve &amp; Execute In-Cluster
                      </button>
                    )}
                    <button className="pf-v5-c-button pf-m-secondary" onClick={() => handleCreateSCMPR(req, 'github')}>
                      Create GitHub PR
                    </button>
                    <button className="pf-v5-c-button pf-m-secondary" onClick={() => handleCreateSCMPR(req, 'gitlab')}>
                      Create GitLab MR
                    </button>
                    <button className="pf-v5-c-button pf-m-danger" onClick={() => handleReject(req)}>
                      Reject
                    </button>
                  </div>
                )}
              </div>
            ))
          )}
        </div>
      )}

      {activeTab === 'gitops' && (
        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '16px' }}>
          <div className="pf-v5-c-card" style={{ padding: '16px', gridColumn: '1 / -1' }}>
            <h3>Cluster Approval Governance Policy</h3>
            <label>
              Approval Mode:{' '}
              <select value={approvalMode} onChange={(e) => setApprovalMode(e.target.value)}>
                <option value="HYBRID">HYBRID (In-Cluster RemediationRequest CRD + GitHub PR / GitLab MR)</option>
                <option value="IN_CLUSTER">IN_CLUSTER (Direct In-Cluster Approval in OpenShift Console)</option>
                <option value="GITHUB_PR">GITHUB_PR (GitHub OAuth Pull Request)</option>
                <option value="GITLAB_MR">GITLAB_MR (GitLab OAuth Merge Request)</option>
              </select>
            </label>{' '}
            <label style={{ marginLeft: '16px' }}>
              <input
                type="checkbox"
                checked={inClusterAllowed}
                onChange={(e) => setInClusterAllowed(e.target.checked)}
              />{' '}
              Allow Direct In-Cluster Approval when resources are managed inside the cluster
            </label>{' '}
            <button className="pf-v5-c-button pf-m-primary" onClick={() => handleSaveGitOpsConfig()}>
              Save Policy
            </button>
          </div>

          <div className="pf-v5-c-card" style={{ padding: '16px' }}>
            <h3>
              GitHub OAuth App Integration ({gitopsStatus?.github?.connected ? `Connected: ${gitopsStatus.github.connectedUser}` : 'Not Connected'})
            </h3>
            <div>
              <label>GitHub OAuth Client ID</label>
              <input value={ghClientId} onChange={(e) => setGhClientId(e.target.value)} style={{ width: '100%' }} />
              <label>GitHub OAuth Client Secret</label>
              <input
                type="password"
                value={ghClientSecret}
                onChange={(e) => setGhClientSecret(e.target.value)}
                style={{ width: '100%' }}
              />
              <label>Repository (owner/repo)</label>
              <input value={ghRepo} onChange={(e) => setGhRepo(e.target.value)} style={{ width: '100%' }} />
              <label>Base Branch</label>
              <input value={ghBranch} onChange={(e) => setGhBranch(e.target.value)} style={{ width: '100%' }} />
            </div>
            <div style={{ display: 'flex', gap: '8px', marginTop: '12px' }}>
              <button className="pf-v5-c-button pf-m-secondary" onClick={() => handleSaveGitOpsConfig('github')}>
                Save GitHub Config
              </button>
              <button className="pf-v5-c-button pf-m-primary" onClick={() => handleStartOAuth('github')}>
                Connect with GitHub OAuth
              </button>
              <button className="pf-v5-c-button pf-m-danger" onClick={() => handleDisconnectOAuth('github')}>
                Disconnect
              </button>
            </div>
          </div>

          <div className="pf-v5-c-card" style={{ padding: '16px' }}>
            <h3>
              GitLab OAuth App Integration ({gitopsStatus?.gitlab?.connected ? `Connected: ${gitopsStatus.gitlab.connectedUser}` : 'Not Connected'})
            </h3>
            <div>
              <label>GitLab Instance URL</label>
              <input value={glUrl} onChange={(e) => setGlUrl(e.target.value)} style={{ width: '100%' }} />
              <label>GitLab Application ID</label>
              <input value={glClientId} onChange={(e) => setGlClientId(e.target.value)} style={{ width: '100%' }} />
              <label>GitLab Client Secret</label>
              <input
                type="password"
                value={glClientSecret}
                onChange={(e) => setGlClientSecret(e.target.value)}
                style={{ width: '100%' }}
              />
              <label>Project Path (group/project)</label>
              <input value={glRepo} onChange={(e) => setGlRepo(e.target.value)} style={{ width: '100%' }} />
              <label>Base Branch</label>
              <input value={glBranch} onChange={(e) => setGlBranch(e.target.value)} style={{ width: '100%' }} />
            </div>
            <div style={{ display: 'flex', gap: '8px', marginTop: '12px' }}>
              <button className="pf-v5-c-button pf-m-secondary" onClick={() => handleSaveGitOpsConfig('gitlab')}>
                Save GitLab Config
              </button>
              <button className="pf-v5-c-button pf-m-primary" onClick={() => handleStartOAuth('gitlab')}>
                Connect with GitLab OAuth
              </button>
              <button className="pf-v5-c-button pf-m-danger" onClick={() => handleDisconnectOAuth('gitlab')}>
                Disconnect
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
};

export default KubeDoctorDashboard;
