package workflowhelper

import (
	"reflect"
	"strings"
	"testing"
)

// graphLock is a dogfood-shaped lockfile with every case the derivation must
// get right: a ranged devDependency pin, a nested copy that shadows a hoisted
// one, a package the run-time dependencies share, an optional platform
// package a lint tool also needs, an absent optional dependency, a peer, and
// an optional peer that only a run-time dependency installs.
const graphLock = `{
  "name": "dogfood",
  "version": "1.0.0",
  "lockfileVersion": 3,
  "requires": true,
  "packages": {
    "": {
      "name": "dogfood",
      "version": "1.0.0",
      "dependencies": {
        "runtime": "1.0.0"
      },
      "devDependencies": {
        "lint": "1.0.0",
        "tool": "^2.0.0"
      }
    },
    "node_modules/lint": {
      "version": "1.0.0",
      "resolved": "https://registry.npmjs.org/lint/-/lint-1.0.0.tgz",
      "integrity": "sha512-lint",
      "dev": true,
      "license": "MIT",
      "dependencies": {
        "native-linux": "2.1.0",
        "nested": "1.0.0"
      }
    },
    "node_modules/maybe": {
      "version": "1.0.0",
      "resolved": "https://registry.npmjs.org/maybe/-/maybe-1.0.0.tgz",
      "integrity": "sha512-maybe",
      "license": "MIT"
    },
    "node_modules/native-linux": {
      "version": "2.1.0",
      "resolved": "https://registry.npmjs.org/native-linux/-/native-linux-2.1.0.tgz",
      "integrity": "sha512-native",
      "cpu": [
        "x64"
      ],
      "dev": true,
      "license": "MIT",
      "os": [
        "linux"
      ]
    },
    "node_modules/nested": {
      "version": "1.0.0",
      "resolved": "https://registry.npmjs.org/nested/-/nested-1.0.0.tgz",
      "integrity": "sha512-nested1",
      "dev": true,
      "license": "MIT"
    },
    "node_modules/peer": {
      "version": "1.0.0",
      "resolved": "https://registry.npmjs.org/peer/-/peer-1.0.0.tgz",
      "integrity": "sha512-peer",
      "dev": true,
      "license": "MIT",
      "peer": true
    },
    "node_modules/runtime": {
      "version": "1.0.0",
      "resolved": "https://registry.npmjs.org/runtime/-/runtime-1.0.0.tgz",
      "integrity": "sha512-runtime",
      "license": "MIT",
      "dependencies": {
        "maybe": "1.0.0",
        "shared": "1.0.0"
      }
    },
    "node_modules/shared": {
      "version": "1.0.0",
      "resolved": "https://registry.npmjs.org/shared/-/shared-1.0.0.tgz",
      "integrity": "sha512-shared",
      "license": "MIT"
    },
    "node_modules/tool": {
      "version": "2.1.0",
      "resolved": "https://registry.npmjs.org/tool/-/tool-2.1.0.tgz",
      "integrity": "sha512-tool",
      "dev": true,
      "license": "MIT",
      "bin": {
        "tool": "bin/tool"
      },
      "dependencies": {
        "nested": "^2.0.0",
        "shared": "^1.0.0"
      },
      "engines": {
        "node": ">=18"
      },
      "optionalDependencies": {
        "native-gone": "2.1.0",
        "native-linux": "2.1.0"
      },
      "peerDependencies": {
        "maybe": "^1.0.0",
        "peer": "^1.0.0"
      },
      "peerDependenciesMeta": {
        "maybe": {
          "optional": true
        }
      }
    },
    "node_modules/tool/node_modules/nested": {
      "version": "2.0.0",
      "resolved": "https://registry.npmjs.org/nested/-/nested-2.0.0.tgz",
      "integrity": "sha512-nested2",
      "dev": true,
      "license": "MIT"
    }
  }
}
`

// TestTypeScriptToolchainFollowsTheLockfileGraph pins the derived lockfile
// byte for byte: npm's own layout, only what the tools install, and the
// dev/optional/peer flags of a package with nothing but devDependencies.
func TestTypeScriptToolchainFollowsTheLockfileGraph(t *testing.T) {
	pins, lock, err := deriveTypeScriptToolchain([]byte(graphLock), "starter", []string{"tool"})
	if err != nil {
		t.Fatal(err)
	}
	// The dogfood manifest says ^2.0.0; the scaffold pins what it resolves to.
	if want := []pinnedPackage{{Name: "tool", Version: "2.1.0"}}; !reflect.DeepEqual(pins, want) {
		t.Fatalf("pins = %+v, want %+v", pins, want)
	}
	want := `{
  "name": "starter",
  "version": "0.1.0",
  "lockfileVersion": 3,
  "requires": true,
  "packages": {
    "": {
      "name": "starter",
      "version": "0.1.0",
      "devDependencies": {
        "tool": "2.1.0"
      },
      "engines": {
        "node": ">=22.7"
      }
    },
    "node_modules/native-linux": {
      "version": "2.1.0",
      "resolved": "https://registry.npmjs.org/native-linux/-/native-linux-2.1.0.tgz",
      "integrity": "sha512-native",
      "cpu": [
        "x64"
      ],
      "dev": true,
      "license": "MIT",
      "optional": true,
      "os": [
        "linux"
      ]
    },
    "node_modules/peer": {
      "version": "1.0.0",
      "resolved": "https://registry.npmjs.org/peer/-/peer-1.0.0.tgz",
      "integrity": "sha512-peer",
      "dev": true,
      "license": "MIT",
      "peer": true
    },
    "node_modules/shared": {
      "version": "1.0.0",
      "resolved": "https://registry.npmjs.org/shared/-/shared-1.0.0.tgz",
      "integrity": "sha512-shared",
      "dev": true,
      "license": "MIT"
    },
    "node_modules/tool": {
      "version": "2.1.0",
      "resolved": "https://registry.npmjs.org/tool/-/tool-2.1.0.tgz",
      "integrity": "sha512-tool",
      "dev": true,
      "license": "MIT",
      "bin": {
        "tool": "bin/tool"
      },
      "dependencies": {
        "nested": "^2.0.0",
        "shared": "^1.0.0"
      },
      "engines": {
        "node": ">=18"
      },
      "optionalDependencies": {
        "native-gone": "2.1.0",
        "native-linux": "2.1.0"
      },
      "peerDependencies": {
        "maybe": "^1.0.0",
        "peer": "^1.0.0"
      },
      "peerDependenciesMeta": {
        "maybe": {
          "optional": true
        }
      }
    },
    "node_modules/tool/node_modules/nested": {
      "version": "2.0.0",
      "resolved": "https://registry.npmjs.org/nested/-/nested-2.0.0.tgz",
      "integrity": "sha512-nested2",
      "dev": true,
      "license": "MIT"
    }
  }
}
`
	if string(lock) != want {
		t.Fatalf("derived lockfile:\n%s\nwant:\n%s", lock, want)
	}
}

// TestTypeScriptToolchainRefusesWhatItCannotPin: a lockfile the scaffold
// cannot pin from fails `wefty workflow init` and this package's tests, never
// writes a starter with a floating or broken toolchain.
func TestTypeScriptToolchainRefusesWhatItCannotPin(t *testing.T) {
	for _, test := range []struct {
		name  string
		lock  string
		tools []string
		want  string
	}{
		{"not a devDependency", graphLock, []string{"runtime"}, "runtime is not one of the lockfile's devDependencies"},
		{"not installed", strings.Replace(graphLock, `"node_modules/tool": {`, `"node_modules/elsewhere": {`, 1), []string{"tool"}, "does not install tool"},
		{"missing dependency", strings.Replace(graphLock, `"node_modules/shared": {`, `"node_modules/unshared": {`, 1), []string{"tool"}, `does not install shared, which "node_modules/tool" depends on`},
		{"not an exact version", strings.Replace(graphLock, `"version": "2.1.0",
      "resolved": "https://registry.npmjs.org/tool/`, `"version": "latest",
      "resolved": "https://registry.npmjs.org/tool/`, 1), []string{"tool"}, `resolves tool to "latest"`},
		{"local tarball tool", strings.Replace(graphLock, `"resolved": "https://registry.npmjs.org/tool/-/tool-2.1.0.tgz"`, `"resolved": "file:../tool-2.1.0.tgz"`, 1), []string{"tool"}, `resolves "node_modules/tool" from "file:../tool-2.1.0.tgz"`},
		{"local tarball dependency", strings.Replace(graphLock, `"resolved": "https://registry.npmjs.org/shared/-/shared-1.0.0.tgz"`, `"resolved": "file:../shared-1.0.0.tgz"`, 1), []string{"tool"}, `resolves "node_modules/shared" from "file:../shared-1.0.0.tgz"`},
		{"older lockfile", strings.Replace(graphLock, `"lockfileVersion": 3`, `"lockfileVersion": 2`, 1), []string{"tool"}, "version 2"},
	} {
		t.Run(test.name, func(t *testing.T) {
			if test.lock == graphLock && test.name != "not a devDependency" {
				t.Fatal("the fixture edit did not apply")
			}
			_, _, err := deriveTypeScriptToolchain([]byte(test.lock), "starter", test.tools)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("err = %v, want one mentioning %q", err, test.want)
			}
		})
	}
}
