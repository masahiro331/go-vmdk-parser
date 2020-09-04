package detector

import (
	"fmt"
	"sort"
	"time"

	"github.com/aquasecurity/fanal/types"
	"github.com/aquasecurity/trivy/pkg/detector/library"
	"github.com/aquasecurity/trivy/pkg/detector/ospkg"
	"github.com/masahiro331/trivy/pkg/report"
	"golang.org/x/xerrors"
	"pkg/mod/github.com/pkg/errors@v0.8.1"
)

func DetectOSVulnerability(imageName, osFamily, osName string, created time.Time, pkgs []ftypes.Package) {
	detector := ospkg.Detector{}

	if osFamily == "" {
		return nil, false, nil
	}
	vulns, eosl, err := ospkgDetector.Detect("", osFamily, osName, time.Time{}, pkgs)
	if err == ospkgDetector.ErrUnsupportedOS {
		return nil, false, nil
	} else if err != nil {
		return nil, false, errors.Wrapf(err, "failed vulnerability detection of OS packages")
	}

	vmDetail := fmt.Sprintf("%s (%s %s)", target, osFamily, osName)
}
func DetectLibVulnerability(imageName, filePath string, created time.Time, pkgs []types.LibraryInfo) {
	driverFactory := library.DriverFactory{}
	libDetector := library.NewDetector(driverFactory)

	var results report.Results
	for _, app := range apps {
		vulns, err := libDetector.Detect("", app.FilePath, time.Time{}, app.Libraries)
		if err != nil {
			return nil, xerrors.Errorf("failed vulnerability detection of libraries: %w", err)
		}

		results = append(results, report.Result{
			Target:          app.FilePath,
			Vulnerabilities: vulns,
			Type:            app.Type,
		})
	}
	sort.Slice(results, func(i, j int) bool {
		return results[i].Target < results[j].Target
	})
	return results, nil
}
