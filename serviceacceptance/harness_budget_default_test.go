//go:build service_acceptance && !service_acceptance_realtiming && (darwin || linux)

package serviceacceptance

// The fast service acceptance lane runs the production L1 build.
const controlPlaneBuildTags = ""
