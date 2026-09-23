package lfs

// MediaType is the content type every Git LFS API request and response carries.
// A client that does not accept it gets a 406, and a response that omits it is
// not recognized as an LFS answer.
const MediaType = "application/vnd.git-lfs+json"

// ErrorBody is the top-level failure body. It is the shape git-lfs prints to
// the user, so Message is read by a person, not a program.
type ErrorBody struct {
	Message string `json:"message"`
	// RequestID lets an operator tie a client-side failure to a server log
	// line. It is optional in the protocol.
	RequestID string `json:"request_id,omitempty"`
	// DocumentationURL points at an explanation of the failure.
	DocumentationURL string `json:"documentation_url,omitempty"`
}
