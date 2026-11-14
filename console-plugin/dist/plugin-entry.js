// OpenShift 4.22+ Dynamic Console Plugin Entrypoint (kube-doctor-console-plugin)
window.loadPluginEntry("kube-doctor-console-plugin@1.35.0", {
  exposedModules: {
    KubeDoctorDashboard: "./src/components/KubeDoctorDashboard"
  }
});
