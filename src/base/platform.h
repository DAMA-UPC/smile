

#ifndef _BASE_PLATFORM_H_
#define _BASE_PLATFORM_H_ 

#include "../base/macros.h"
#include "../base/types.h"


SMILE_NS_BEGIN

class Platform {
    public:
        static std::size_t getSystemPageSize() noexcept;

        static std::size_t roundSizeToPageSize( std::size_t size ) noexcept;
};

SMILE_NS_END

#endif /* ifndef _BASE_PLATFORM_H_ */
