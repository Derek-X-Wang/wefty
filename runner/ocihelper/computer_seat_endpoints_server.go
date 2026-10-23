package ocihelper

func (server *Server) serveSeatEndpoint(session *serverSession, operation *sessionOperation, wire *framedConn, request frame) {
	var body DialSeatEndpointRequest
	if !decodeRequest(wire, request.Body, &body) {
		return
	}
	if body.Generation == 0 || body.Place < 1 || body.Place > 4 || body.Name == "" {
		_ = writeFailure(wire, CodeInvalidRequest, "Seat generation, place and endpoint name are required")
		return
	}
	attempt, rpcErr := session.authorizeAttempt(body.Authority)
	if rpcErr != nil {
		_ = writeRPCError(wire, rpcErr)
		return
	}
	if !attempt.computer {
		_ = writeFailure(wire, CodeUnauthorizedAttempt, "Seat endpoints require a live Computer attempt")
		return
	}
	engine, ok := server.engine.(ComputerSeatEndpointEngine)
	if !ok {
		_ = writeFailure(wire, CodeUnsupportedOperation, "Computer Seat endpoints unavailable")
		return
	}
	stream := &backendReadyOperationStream{
		operationStream: &operationStream{Conn: operation.conn, cancel: operation.cancel}, wire: wire,
		acknowledged: operation.monitorAcknowledgement(), ctx: operation.ctx, method: request.Method,
		marker: attemptPortBackendReady, writeSetup: func() error { return writeSuccess(wire, struct{}{}) },
	}
	if err := engine.DialSeatEndpoint(operation.ctx, body, stream); err != nil && !stream.ready {
		_ = writeSeatOperationFailure(wire, err)
	}
}
