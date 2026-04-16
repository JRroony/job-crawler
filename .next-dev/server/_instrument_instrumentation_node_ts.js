"use strict";
/*
 * ATTENTION: An "eval-source-map" devtool has been used.
 * This devtool is neither made for production nor for readable output files.
 * It uses "eval()" calls to create a separate source file with attached SourceMaps in the browser devtools.
 * If you are trying to read the output file, select a different devtool (https://webpack.js.org/configuration/devtool/)
 * or disable the default devtool with "devtool: false".
 * If you are looking for production-ready output files, see mode: "production" (https://webpack.js.org/configuration/mode/).
 */
exports.id = "_instrument_instrumentation_node_ts";
exports.ids = ["_instrument_instrumentation_node_ts"];
exports.modules = {

/***/ "(instrument)/./instrumentation.node.ts":
/*!*********************************!*\
  !*** ./instrumentation.node.ts ***!
  \*********************************/
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

eval("__webpack_require__.r(__webpack_exports__);\n/* harmony export */ __webpack_require__.d(__webpack_exports__, {\n/* harmony export */   registerNodeInstrumentation: () => (/* binding */ registerNodeInstrumentation)\n/* harmony export */ });\n/* harmony import */ var server_only__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(/*! server-only */ \"(instrument)/./node_modules/next/dist/compiled/server-only/empty.js\");\n/* harmony import */ var server_only__WEBPACK_IMPORTED_MODULE_0___default = /*#__PURE__*/__webpack_require__.n(server_only__WEBPACK_IMPORTED_MODULE_0__);\n\nasync function registerNodeInstrumentation() {\n    const { startRecurringBackgroundIngestionScheduler } = await Promise.all(/*! import() */[__webpack_require__.e(\"vendor-chunks/zod\"), __webpack_require__.e(\"_instrument_lib_server_background_recurring-ingestion_ts\")]).then(__webpack_require__.bind(__webpack_require__, /*! @/lib/server/background/recurring-ingestion */ \"(instrument)/./lib/server/background/recurring-ingestion.ts\"));\n    startRecurringBackgroundIngestionScheduler();\n}\n//# sourceURL=[module]\n//# sourceMappingURL=data:application/json;charset=utf-8;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiKGluc3RydW1lbnQpLy4vaW5zdHJ1bWVudGF0aW9uLm5vZGUudHMiLCJtYXBwaW5ncyI6Ijs7Ozs7O0FBQXFCO0FBRWQsZUFBZUE7SUFDcEIsTUFBTSxFQUFFQywwQ0FBMEMsRUFBRSxHQUFHLE1BQU0saVVBQzNEO0lBRUZBO0FBQ0YiLCJzb3VyY2VzIjpbIkM6XFxQcm9qZWN0XFxqb2IgY3Jhd2xlclxcaW5zdHJ1bWVudGF0aW9uLm5vZGUudHMiXSwic291cmNlc0NvbnRlbnQiOlsiaW1wb3J0IFwic2VydmVyLW9ubHlcIjtcblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIHJlZ2lzdGVyTm9kZUluc3RydW1lbnRhdGlvbigpIHtcbiAgY29uc3QgeyBzdGFydFJlY3VycmluZ0JhY2tncm91bmRJbmdlc3Rpb25TY2hlZHVsZXIgfSA9IGF3YWl0IGltcG9ydChcbiAgICBcIkAvbGliL3NlcnZlci9iYWNrZ3JvdW5kL3JlY3VycmluZy1pbmdlc3Rpb25cIlxuICApO1xuICBzdGFydFJlY3VycmluZ0JhY2tncm91bmRJbmdlc3Rpb25TY2hlZHVsZXIoKTtcbn1cbiJdLCJuYW1lcyI6WyJyZWdpc3Rlck5vZGVJbnN0cnVtZW50YXRpb24iLCJzdGFydFJlY3VycmluZ0JhY2tncm91bmRJbmdlc3Rpb25TY2hlZHVsZXIiXSwiaWdub3JlTGlzdCI6W10sInNvdXJjZVJvb3QiOiIifQ==\n//# sourceURL=webpack-internal:///(instrument)/./instrumentation.node.ts\n");

/***/ })

};
;