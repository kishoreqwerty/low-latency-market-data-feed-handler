# Applies the requested sanitizer set to an INTERFACE target.
#
# ASan/UBSan and TSan are mutually exclusive within a single binary, so the
# `debug` build configuration picks one via -DMDFH_SANITIZER=address|thread|none
# rather than enabling both simultaneously.
function(mdfh_apply_sanitizers target sanitizer)
  if(sanitizer STREQUAL "address")
    target_compile_options(${target} INTERFACE -fsanitize=address,undefined -fno-omit-frame-pointer -g -O1)
    target_link_options(${target} INTERFACE -fsanitize=address,undefined)
  elseif(sanitizer STREQUAL "thread")
    target_compile_options(${target} INTERFACE -fsanitize=thread -fno-omit-frame-pointer -g -O1)
    target_link_options(${target} INTERFACE -fsanitize=thread)
  elseif(sanitizer STREQUAL "none")
    target_compile_options(${target} INTERFACE -g -O0)
  else()
    message(FATAL_ERROR "Unknown MDFH_SANITIZER value: ${sanitizer} (expected address|thread|none)")
  endif()
endfunction()
