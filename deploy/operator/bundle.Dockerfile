FROM scratch

LABEL operators.operatorframework.io.bundle.mediatype.v1=registry+v1
LABEL operators.operatorframework.io.bundle.manifests.v1=manifests/
LABEL operators.operatorframework.io.bundle.metadata.v1=metadata/
LABEL operators.operatorframework.io.bundle.package.v1=kube-doctor-operator
LABEL operators.operatorframework.io.bundle.channels.v1=stable
LABEL operators.operatorframework.io.bundle.channel.default.v1=stable
LABEL com.redhat.openshift.versions="v4.22"

COPY deploy/crd/remediationrequest.kubedoctor.io.yaml /manifests/remediationrequest.kubedoctor.io.yaml
COPY deploy/crd/kubedoctor.kubedoctor.io.yaml /manifests/kubedoctor.kubedoctor.io.yaml
COPY deploy/operator/clusterserviceversion.yaml /manifests/kube-doctor-operator.clusterserviceversion.yaml
COPY deploy/operator/metadata/annotations.yaml /metadata/annotations.yaml
