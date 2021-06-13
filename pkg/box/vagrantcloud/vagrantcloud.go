package vagrantcloud

import (
	"archive/tar"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"

	"github.com/jochasinga/requests"
	"golang.org/x/xerrors"
)

var urlFormat = "https://vagrantcloud.com/%s"

const (
	// Providers
	Parallels     = "parallels"
	Hyperv        = "hyperv"
	Libvirt       = "libvirt"
	Virtualbox    = "virtualbox"
	VmwareDesktop = "vmware_desktop"
)

type BoxJson struct {
	Description      string `json:"description"`
	ShortDescription string `json:"short_description"`
	Name             string `json:"name"`
	Versions         []struct {
		Version             string `json:"version"`
		Status              string `json:"status"`
		DescriptionHTML     string `json:"description_html"`
		DescriptionMarkdown string `json:"description_markdown"`
		Providers           []struct {
			Name         string `json:"name"`
			URL          string `json:"url"`
			Checksum     string `json:"checksum"`
			ChecksumType string `json:"checksum_type"`
		} `json:"providers"`
	} `json:"versions"`
}

func GetBox(boxName, version string) (io.ReadCloser, error) {
	addMimeType := func(r *requests.Request) {
		r.Header.Add("Accept", "*/*")
	}

	resp, err := requests.Get(fmt.Sprintf(urlFormat, boxName), addMimeType)
	if err != nil {
		return nil, xerrors.Errorf("failed to get boxes: %w", err)
	}
	defer resp.Body.Close()
	var box BoxJson
	if err := json.NewDecoder(resp.Body).Decode(&box); err != nil {
		return nil, xerrors.Errorf("failed to decode boxes: %w", err)
	}

	for _, v := range box.Versions {
		if v.Version == version || version == "" {
			for _, provider := range v.Providers {
				if provider.Name == Virtualbox {
					resp, err := requests.Get(provider.URL, addMimeType)
					if err != nil {
						return nil, xerrors.Errorf("failed to get box error: %w", err)
					}
					return resp.Body, nil
				}
			}
		}
	}

	return nil, nil
}

func NewBoxReader(reader io.Reader) (*tar.Reader, error) {
	greader, err := gzip.NewReader(reader)
	if err != nil {
		return tar.NewReader(greader), nil
	}

	treader := tar.NewReader(greader)
	if err != nil {
		return nil, xerrors.Errorf("failed to NewTarReader: %w", err)
	}
	return treader, err
}
