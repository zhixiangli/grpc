# Copyright 2026 gRPC authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from libc.stdint cimport uint32_t
from libc.string cimport memcpy


cdef const char *_NATIVE_DESERIALIZER_CAPSULE_NAME = (
    "grpc.python.native_deserializer.v1"
)
cdef uint32_t _NATIVE_DESERIALIZER_ABI_VERSION = 1


cdef struct grpc_python_slice:
    const char *data
    size_t length


ctypedef int (*_native_parse_slices_fn)(
    const grpc_python_slice *slices,
    size_t nslices,
    void *user_data,
    void **parsed_out,
    char **error_out,
) noexcept nogil


ctypedef object (*_native_materialize_fn)(
    void *parsed,
    void *user_data,
) except *


ctypedef void (*_native_destroy_fn)(void *value) noexcept nogil


cdef struct grpc_python_native_deserializer_v1:
    uint32_t abi_version
    void *user_data
    _native_parse_slices_fn parse_slices
    _native_materialize_fn materialize
    _native_destroy_fn destroy_parsed
    _native_destroy_fn destroy_user_data


cdef struct _test_native_parsed:
    char *data
    size_t length


cdef int _test_native_parse_slices(
    const grpc_python_slice *slices,
    size_t nslices,
    void *user_data,
    void **parsed_out,
    char **error_out,
) noexcept nogil:
    cdef size_t i
    cdef size_t offset = 0
    cdef size_t total = 0
    cdef _test_native_parsed *parsed

    for i in range(nslices):
        total += slices[i].length

    parsed = <_test_native_parsed *>gpr_malloc(sizeof(_test_native_parsed))
    if parsed == NULL:
        return 1
    parsed.data = <char *>gpr_malloc(total)
    if parsed.data == NULL and total != 0:
        gpr_free(parsed)
        return 1
    parsed.length = total

    for i in range(nslices):
        if slices[i].length:
            memcpy(parsed.data + offset, slices[i].data, slices[i].length)
            offset += slices[i].length

    parsed_out[0] = <void *>parsed
    return 0


cdef object _test_native_materialize(
    void *parsed_ptr,
    void *user_data,
) except *:
    cdef _test_native_parsed *parsed = <_test_native_parsed *>parsed_ptr
    return parsed.data[:parsed.length]


cdef void _test_native_destroy_parsed(void *parsed_ptr) noexcept nogil:
    cdef _test_native_parsed *parsed = <_test_native_parsed *>parsed_ptr
    if parsed != NULL:
        if parsed.data != NULL:
            gpr_free(parsed.data)
        gpr_free(parsed)


cdef grpc_python_native_deserializer_v1 _TEST_NATIVE_DESERIALIZER


cdef class NativeDeserializer:
    cdef grpc_python_native_deserializer_v1 *_descriptor
    cdef object _capsule

    def __cinit__(self, object capsule):
        cdef void *descriptor = cpython.PyCapsule_GetPointer(
            capsule, _NATIVE_DESERIALIZER_CAPSULE_NAME
        )
        if descriptor == NULL:
            raise ValueError("Invalid native deserializer capsule")
        self._descriptor = <grpc_python_native_deserializer_v1 *>descriptor
        if self._descriptor.abi_version != _NATIVE_DESERIALIZER_ABI_VERSION:
            raise ValueError("Unsupported native deserializer ABI version")
        if self._descriptor.parse_slices == NULL:
            raise ValueError("Native deserializer missing parse_slices")
        if self._descriptor.materialize == NULL:
            raise ValueError("Native deserializer missing materialize")
        self._capsule = capsule

    cdef object deserialize_byte_buffer(self, grpc_byte_buffer *byte_buffer):
        cdef grpc_byte_buffer_reader message_reader
        cdef bint message_reader_status
        cdef grpc_slice message_slice
        cdef size_t message_slice_length
        cdef size_t message_length = grpc_byte_buffer_length(byte_buffer)
        cdef size_t offset = 0
        cdef char *message = NULL
        cdef grpc_python_slice native_slice
        cdef void *parsed = NULL
        cdef char *error = NULL
        cdef int parse_result
        cdef object result

        if message_length:
            message = <char *>gpr_malloc(message_length)
            if message == NULL:
                raise MemoryError()

        message_reader_status = grpc_byte_buffer_reader_init(
            &message_reader, byte_buffer
        )
        if not message_reader_status:
            if message != NULL:
                gpr_free(message)
            return None

        try:
            while grpc_byte_buffer_reader_next(&message_reader, &message_slice):
                message_slice_length = grpc_slice_length(message_slice)
                if message_slice_length > 0:
                    memcpy(
                        message + offset,
                        <char *>grpc_slice_start_ptr(message_slice),
                        message_slice_length,
                    )
                    offset += message_slice_length
                grpc_slice_unref(message_slice)
        finally:
            grpc_byte_buffer_reader_destroy(&message_reader)

        native_slice.data = <const char *>message
        native_slice.length = message_length

        with nogil:
            parse_result = self._descriptor.parse_slices(
                &native_slice,
                1,
                self._descriptor.user_data,
                &parsed,
                &error,
            )

        if message != NULL:
            gpr_free(message)

        if parse_result != 0:
            if error != NULL:
                gpr_free(error)
            raise ValueError("Native deserializer parse failed")

        try:
            result = self._descriptor.materialize(
                parsed, self._descriptor.user_data
            )
            return result
        finally:
            if parsed != NULL and self._descriptor.destroy_parsed != NULL:
                with nogil:
                    self._descriptor.destroy_parsed(parsed)


def is_native_deserializer(object deserializer):
    return isinstance(deserializer, NativeDeserializer)


def _test_native_deserializer():
    _TEST_NATIVE_DESERIALIZER.abi_version = _NATIVE_DESERIALIZER_ABI_VERSION
    _TEST_NATIVE_DESERIALIZER.user_data = NULL
    _TEST_NATIVE_DESERIALIZER.parse_slices = _test_native_parse_slices
    _TEST_NATIVE_DESERIALIZER.materialize = _test_native_materialize
    _TEST_NATIVE_DESERIALIZER.destroy_parsed = _test_native_destroy_parsed
    _TEST_NATIVE_DESERIALIZER.destroy_user_data = NULL
    return cpython.PyCapsule_New(
        &_TEST_NATIVE_DESERIALIZER, _NATIVE_DESERIALIZER_CAPSULE_NAME, NULL
    )
