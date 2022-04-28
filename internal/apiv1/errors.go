package apiv1

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/go-chi/render"
	"github.com/go-playground/validator/v10"
)

var (
	errJsonUnmarshal = fmt.Errorf("unmarshal err")
)

type Error struct {
	Msg    string        `json:"msg"`
	Code   string        `json:"code,omitempty"`
	Errors []ErrorDetail `json:"errors,omitempty"`
}

type ErrorDetail struct {
	Field string `json:"field"`
	Rule  string `json:"rule"`
	Value string `json:"value"`
}

type ErrResponseOpt func(e *Error)

func ErrResponseWithCode(code string) ErrResponseOpt {
	return func(e *Error) {
		e.Code = code
	}
}

func ErrResponseWithMsg(msg string) ErrResponseOpt {
	return func(e *Error) {
		e.Msg = msg
	}
}

func ErrResponseWithDetails(errors []ErrorDetail) ErrResponseOpt {
	return func(e *Error) {
		e.Errors = errors
	}
}

func NewErrResponse400(opts ...ErrResponseOpt) (Error, int) {
	res := Error{Msg: "Bad Request"}
	for _, o := range opts {
		o(&res)
	}

	return res, http.StatusBadRequest
}

func NewErrResponse401(opts ...ErrResponseOpt) (Error, int) {
	res := Error{Msg: "Unauthorized"}
	for _, o := range opts {
		o(&res)
	}

	return res, http.StatusUnauthorized
}

func NewErrResponse404(opts ...ErrResponseOpt) (Error, int) {
	res := Error{Msg: "Not Found"}
	for _, o := range opts {
		o(&res)
	}

	return res, http.StatusNotFound
}

func NewErrResponse422(opts ...ErrResponseOpt) (Error, int) {
	res := Error{Msg: "Unprocessable Entity"}
	for _, o := range opts {
		o(&res)
	}

	return res, http.StatusUnprocessableEntity
}

func NewErrResponse500(opts ...ErrResponseOpt) (Error, int) {
	res := Error{Msg: "Internal Server Error"}
	for _, o := range opts {
		o(&res)
	}

	return res, http.StatusInternalServerError
}

func SendErrResponse(rw http.ResponseWriter, r *http.Request, status int, body Error) {
	render.Status(r, status)
	render.Respond(rw, r, body)
}

type APIError interface {
	APIErr() (Error, int)
}

func handleErr(rw http.ResponseWriter, r *http.Request, err error) {
	fmt.Println(err)
	var validationErr validator.ValidationErrors

	switch true {
	case errors.Is(err, errJsonUnmarshal):
		resp, status := NewErrResponse400(ErrResponseWithMsg(err.Error()))
		SendErrResponse(rw, r, status, resp)
	case errors.As(err, &validationErr):
		errDetails := make([]ErrorDetail, len(validationErr))
		for i, v := range validationErr {
			errDetails[i] = ErrorDetail{

				Field: v.StructNamespace(),
				Rule:  v.Tag(),
				Value: v.Param(),
			}
		}

		resp, status := NewErrResponse422(ErrResponseWithMsg("Validation error"), ErrResponseWithDetails(errDetails))
		SendErrResponse(rw, r, status, resp)
	default:
		resp, status := NewErrResponse500()
		SendErrResponse(rw, r, status, resp)
	}
}
