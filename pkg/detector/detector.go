package detector

import (
	"time"

	ftypes "github.com/aquasecurity/fanal/types"
	"github.com/aquasecurity/trivy/pkg/detector/ospkg"
	"github.com/aquasecurity/trivy/pkg/types"
	"golang.org/x/xerrors"
)

func DetectOSVulnerability(imageName, osFamily, osName string, _, pkgs []ftypes.Package) ([]types.DetectedVulnerability, bool, error) {
	detector := ospkg.Detector{}

	if osFamily == "" {
		return nil, false, nil
	}
	vulns, eosl, err := detector.Detect("", osFamily, osName, time.Time{}, pkgs)
	if err == ospkg.ErrUnsupportedOS {
		return nil, false, nil
	} else if err != nil {
		return nil, false, xerrors.Errorf("failed vulnerability detection of OS packages: %w", err)
	}
	return vulns, eosl, nil
}

// func DetectLibVulnerability(imageName, filePath string, created time.Time, pkgs []types.LibraryInfo) {
// 	driverFactory := library.DriverFactory{}
// 	libDetector := library.NewDetector(driverFactory)
//
// 	var results report.Results
// 	for _, app := range apps {
// 		vulns, err := libDetector.Detect("", app.FilePath, time.Time{}, app.Libraries)
// 		if err != nil {
// 			return nil, xerrors.Errorf("failed vulnerability detection of libraries: %w", err)
// 		}
//
// 		results = append(results, report.Result{
// 			Target:          app.FilePath,
// 			Vulnerabilities: vulns,
// 			Type:            app.Type,
// 		})
// 	}
// 	sort.Slice(results, func(i, j int) bool {
// 		return results[i].Target < results[j].Target
// 	})
// 	return results, nil
// }
