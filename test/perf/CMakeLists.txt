
#get_filename_component(<VAR> FileName
#        PATH|ABSOLUTE|NAME|EXT|NAME_WE|REALPATH
#        [CACHE])
FILE(GLOB_RECURSE ALL_SRC *.cpp)
# AUX_SOURCE_DIRECTORY 邀ｻ莨ｼ蜉溯♧
FOREACH (F ${ALL_SRC})
    get_filename_component(prjName ${F} NAME_WE)
    MESSAGE("Build ${prjName} according to ${F}")
    ADD_EXECUTABLE(${prjName} ${F})
    TARGET_LINK_LIBRARIES(${prjName} common pthread dl observer_static)

ENDFOREACH (F)