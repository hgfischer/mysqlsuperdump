SOURCES    := $(shell find . -type f -name '*.go')
GOTOOLDIR  := $(shell go env GOTOOLDIR)
LINT       := $(GOBIN)/golint
GODOCDOWN  := $(GOBIN)/godocdown
GOCOV      := $(GOBIN)/gocov
GOCOVHTML  := $(GOBIN)/gocov-html
COVER      := $(GOTOOLDIR)/cover
VET        := $(GOTOOLDIR)/vet
PKGS       := $(shell go list ./...)
PKG        := $(shell go list)
COVER_OUT  := coverage.out
COVER_HTML := coverage.html
COVER_JSON := coverage.json
TMP_COVER  := tmp_cover.out


.PHONY: default
default: test


.PHONY: check_gopath
check_gopath:
ifndef GOPATH
	@echo "ERROR!! GOPATH must be declared. Check http://golang.org/doc/code.html#GOPATH"
	@exit 1
endif
ifeq ($(shell go list ./... | grep -q '^_'; echo $$?), 0)
	@echo "ERROR!! This directory should be at $(GOPATH)/src/$(REPO)"
	@exit 1
endif
	@exit 0


.PHONY: check_gobin
check_gobin:
ifndef GOBIN
	@echo "ERROR!! GOBIN must be declared. Check http://golang.org/doc/code.html#GOBIN"
	@exit 1
endif
	@exit 0


.PHONY: clean
clean: check_gopath
	@echo "Removing temp files..."
	@rm -fv *.cover *.out *.html
	@go clean -v


.PHONY: test
test: $(SYMLINK) check_gopath
	@go get -t
	@for pkg in $(PKGS); do go test -v -race $$pkg || exit 1; done


$(COVER): check_gopath check_gobin
	@go get -v -u golang.org/x/tools/cmd/cover || exit 0

.PHONY: cover
cover: check_gopath $(COVER) $(GOCOV) $(GOCOVHTML)
	@echo 'mode: set' > $(COVER_OUT)
	@touch $(TMP_COVER)
	@for pkg in $(PKGS); do \
		go test -v -coverprofile=$(TMP_COVER) $$pkg || exit 1; \
		grep -v 'mode: set' $(TMP_COVER) >> $(COVER_OUT); \
	done
	@gocov convert $(COVER_OUT) > $(COVER_JSON)
	@gocov report $(COVER_JSON) 
	@gocov-html $(COVER_JSON) > $(COVER_HTML)
	@(which gnome-open && gnome-open $(COVER_HTML)) || (which -s open && open $(COVER_HTML)) || (exit 0)


$(GOCOV): check_gopath check_gobin
	@go get -v -u github.com/axw/gocov/gocov || exit 0


$(GOCOVHTML): check_gopath check_gobin
	@go get -v -u gopkg.in/matm/v1/gocov-html || exit 0


$(LINT): check_gopath check_gobin
	@go get github.com/golang/lint/golint

.PHONY: lint
lint: $(LINT)
	@for src in $(SOURCES); do golint $$src || exit 1; done


.PHONY: check_vet
check_vet:
	@if [ ! -x $(VET) ]; then \
		echo Missing Go vet tool! Please install with the following command...; \
		echo sudo go get code.google.com/p/go.tools/cmd/vet; \
		exit 1; \
	fi

.PHONY: vet
vet: check_gopath check_vet
	@for src in $(SOURCES); do go tool vet $$src; done


$(GODOCDOWN): check_gopath check_gobin
	@go get github.com/robertkrimen/godocdown/godocdown

.PHONY: doc
doc: $(GODOCDOWN)
	@godocdown $(PKG) > GODOC.md
