

#include "../base/platform.h"
#include <unistd.h>

SMILE_NS_BEGIN

std::size_t Platform::getSystemPageSize() noexcept {
  return getpagesize();
}


std::size_t Platform::roundSizeToPageSize( std::size_t size ) noexcept {
  std::size_t remainder = size % getSystemPageSize();
  std::size_t pages = size / getSystemPageSize(); 

  if (remainder == 0) {
    return pages;                                                                                                                     
  }

  return pages + 1;
}  

SMILE_NS_END

