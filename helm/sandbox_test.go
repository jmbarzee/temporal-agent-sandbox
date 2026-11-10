package charts

import (
	"context"
	"slices"
	"strconv"
	"testing"

	helmv1 "github.com/metacensus/library-temporal-kube/gen/helm/v1"
	helm "github.com/metacensus/library-temporal-kube/lib/helm/v1"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/yaml"
)

func TestSandboxChart_KeepsTheSidecarApart(t *testing.T) {
	out, err := helm.NewActivities(FS).RenderChart(context.Background(), &helmv1.RenderChartInput{
		Chart: "sandbox", ReleaseName: "sandbox-sb-1", ValuesYaml: "sandboxId: sb-1\ntaskQueue: sandbox-sb-1\n",
	})
	if err != nil {
		t.Fatal(err)
	}
	var pod corev1.Pod
	if err := yaml.Unmarshal(out.GetResources()[0].GetManifestYaml(), &pod); err != nil {
		t.Fatal(err)
	}
	containers := map[string]corev1.Container{}
	for _, c := range pod.Spec.Containers {
		containers[c.Name] = c
	}
	sandbox, sidecar := containers["sandbox"], containers["sidecar"]
	var shared []string
	for _, m := range sandbox.VolumeMounts {
		if slices.ContainsFunc(sidecar.VolumeMounts, func(s corev1.VolumeMount) bool { return s.Name == m.Name }) {
			shared = append(shared, m.Name)
		}
	}
	sidecarUID := *sidecar.SecurityContext.RunAsUser

	tests := []struct {
		name  string
		holds bool
	}{
		{"success - the containers share no PID namespace", pod.Spec.ShareProcessNamespace == nil || !*pod.Spec.ShareProcessNamespace},
		{"success - commands run as a UID other than the sidecar's", *sandbox.SecurityContext.RunAsUser != sidecarUID},
		{"success - commands cannot gain another UID", !*sandbox.SecurityContext.AllowPrivilegeEscalation},
		{"success - execd answers the sidecar's UID", sandbox.Command[slices.Index(sandbox.Command, "-client-uid")+1] == strconv.FormatInt(sidecarUID, 10)},
		{"success - the containers share only execd's volume", slices.Equal(shared, []string{"execd"})},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if !tt.holds {
				t.Errorf("does not hold in:\n%s", out.GetResources()[0].GetManifestYaml())
			}
		})
	}
}
