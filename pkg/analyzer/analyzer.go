package analyzer

import (
	"io/ioutil"

	"github.com/aquasecurity/fanal/analyzer"
	"github.com/masahiro331/gexto"

	_ "github.com/aquasecurity/fanal/analyzer/command/apk"
	_ "github.com/aquasecurity/fanal/analyzer/library/bundler"
	_ "github.com/aquasecurity/fanal/analyzer/library/cargo"
	_ "github.com/aquasecurity/fanal/analyzer/library/composer"
	_ "github.com/aquasecurity/fanal/analyzer/library/npm"
	_ "github.com/aquasecurity/fanal/analyzer/library/pipenv"
	_ "github.com/aquasecurity/fanal/analyzer/library/poetry"
	_ "github.com/aquasecurity/fanal/analyzer/library/yarn"
	_ "github.com/aquasecurity/fanal/analyzer/os/alpine"
	_ "github.com/aquasecurity/fanal/analyzer/os/amazonlinux"
	_ "github.com/aquasecurity/fanal/analyzer/os/debian"
	_ "github.com/aquasecurity/fanal/analyzer/os/photon"
	_ "github.com/aquasecurity/fanal/analyzer/os/redhatbase"
	_ "github.com/aquasecurity/fanal/analyzer/os/suse"
	_ "github.com/aquasecurity/fanal/analyzer/os/ubuntu"
	_ "github.com/aquasecurity/fanal/analyzer/pkg/apk"
	_ "github.com/aquasecurity/fanal/analyzer/pkg/dpkg"
	_ "github.com/aquasecurity/fanal/analyzer/pkg/rpmcmd"
)

func AnalyzeFileSystem(fs gexto.FileSystem) (*analyzer.AnalysisResult, error) {
	files, err := fs.List()
	if err != nil {
		return nil, err
	}

	result := new(analyzer.AnalysisResult)
	for _, fn := range files {
		ar, err := analyzer.AnalyzeFile(fn, nil, func() ([]byte, error) {
			file, _ := fs.Open(fn)
			return ioutil.ReadAll(file)
		})
		if err != nil {
			return nil, err
		}
		result.Merge(ar)
	}

	return result, nil
}
