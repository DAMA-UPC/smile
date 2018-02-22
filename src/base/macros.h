

#ifndef _SMILE_MACROS_H_


#define SMILE_NS_BEGIN namespace smile {
#define SMILE_NS_END }

#define SMILE_NOT_COPYABLE( classname ) classname( const classname& ) = delete; \
                                        classname& operator=(const classname& ) = delete;

#define SMILE_NOT_INSTANTIABLE( classname ) classname() = delete;

#endif
