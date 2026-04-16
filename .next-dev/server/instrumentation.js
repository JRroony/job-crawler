"use strict";
/*
 * ATTENTION: An "eval-source-map" devtool has been used.
 * This devtool is neither made for production nor for readable output files.
 * It uses "eval()" calls to create a separate source file with attached SourceMaps in the browser devtools.
 * If you are trying to read the output file, select a different devtool (https://webpack.js.org/configuration/devtool/)
 * or disable the default devtool with "devtool: false".
 * If you are looking for production-ready output files, see mode: "production" (https://webpack.js.org/configuration/mode/).
 */
(() => {
var exports = {};
exports.id = "instrumentation";
exports.ids = ["instrumentation"];
exports.modules = {

/***/ "(instrument)/./instrumentation.ts":
/*!****************************!*\
  !*** ./instrumentation.ts ***!
  \****************************/
/***/ ((__unused_webpack_module, __webpack_exports__, __webpack_require__) => {

eval("__webpack_require__.r(__webpack_exports__);\n/* harmony export */ __webpack_require__.d(__webpack_exports__, {\n/* harmony export */   register: () => (/* binding */ register)\n/* harmony export */ });\n/* harmony import */ var server_only__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(/*! server-only */ \"(instrument)/./node_modules/next/dist/compiled/server-only/empty.js\");\n/* harmony import */ var server_only__WEBPACK_IMPORTED_MODULE_0___default = /*#__PURE__*/__webpack_require__.n(server_only__WEBPACK_IMPORTED_MODULE_0__);\n\nasync function register() {\n    // Only run in Node.js runtime (not edge)\n    if (false) {}\n    try {\n        const { registerNodeInstrumentation } = await __webpack_require__.e(/*! import() */ \"_instrument_instrumentation_node_ts\").then(__webpack_require__.bind(__webpack_require__, /*! ./instrumentation.node */ \"(instrument)/./instrumentation.node.ts\"));\n        await registerNodeInstrumentation();\n    } catch (error) {\n        // Log but don't crash the instrumentation\n        console.error(\"[instrumentation] Failed to start background ingestion scheduler\", error);\n    }\n}\n//# sourceURL=[module]\n//# sourceMappingURL=data:application/json;charset=utf-8;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiKGluc3RydW1lbnQpLy4vaW5zdHJ1bWVudGF0aW9uLnRzIiwibWFwcGluZ3MiOiI7Ozs7OztBQUFxQjtBQUVkLGVBQWVBO0lBQ3BCLHlDQUF5QztJQUN6QyxJQUFJQyxLQUFxQyxFQUFFLEVBRTFDO0lBRUQsSUFBSTtRQUNGLE1BQU0sRUFBRUcsMkJBQTJCLEVBQUUsR0FBRyxNQUFNLHdNQUFnQztRQUM5RSxNQUFNQTtJQUNSLEVBQUUsT0FBT0MsT0FBTztRQUNkLDBDQUEwQztRQUMxQ0MsUUFBUUQsS0FBSyxDQUFDLG9FQUFvRUE7SUFDcEY7QUFDRiIsInNvdXJjZXMiOlsiQzpcXFByb2plY3RcXGpvYiBjcmF3bGVyXFxpbnN0cnVtZW50YXRpb24udHMiXSwic291cmNlc0NvbnRlbnQiOlsiaW1wb3J0IFwic2VydmVyLW9ubHlcIjtcblxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIHJlZ2lzdGVyKCkge1xuICAvLyBPbmx5IHJ1biBpbiBOb2RlLmpzIHJ1bnRpbWUgKG5vdCBlZGdlKVxuICBpZiAocHJvY2Vzcy5lbnYuTkVYVF9SVU5USU1FICE9PSBcIm5vZGVqc1wiKSB7XG4gICAgcmV0dXJuO1xuICB9XG5cbiAgdHJ5IHtcbiAgICBjb25zdCB7IHJlZ2lzdGVyTm9kZUluc3RydW1lbnRhdGlvbiB9ID0gYXdhaXQgaW1wb3J0KFwiLi9pbnN0cnVtZW50YXRpb24ubm9kZVwiKTtcbiAgICBhd2FpdCByZWdpc3Rlck5vZGVJbnN0cnVtZW50YXRpb24oKTtcbiAgfSBjYXRjaCAoZXJyb3IpIHtcbiAgICAvLyBMb2cgYnV0IGRvbid0IGNyYXNoIHRoZSBpbnN0cnVtZW50YXRpb25cbiAgICBjb25zb2xlLmVycm9yKFwiW2luc3RydW1lbnRhdGlvbl0gRmFpbGVkIHRvIHN0YXJ0IGJhY2tncm91bmQgaW5nZXN0aW9uIHNjaGVkdWxlclwiLCBlcnJvcik7XG4gIH1cbn1cbiJdLCJuYW1lcyI6WyJyZWdpc3RlciIsInByb2Nlc3MiLCJlbnYiLCJORVhUX1JVTlRJTUUiLCJyZWdpc3Rlck5vZGVJbnN0cnVtZW50YXRpb24iLCJlcnJvciIsImNvbnNvbGUiXSwiaWdub3JlTGlzdCI6W10sInNvdXJjZVJvb3QiOiIifQ==\n//# sourceURL=webpack-internal:///(instrument)/./instrumentation.ts\n");

/***/ }),

/***/ "crypto":
/*!*************************!*\
  !*** external "crypto" ***!
  \*************************/
/***/ ((module) => {

module.exports = require("crypto");

/***/ }),

/***/ "mongodb":
/*!**************************!*\
  !*** external "mongodb" ***!
  \**************************/
/***/ ((module) => {

module.exports = require("mongodb");

/***/ })

};
;

// load runtime
var __webpack_require__ = require("./webpack-runtime.js");
__webpack_require__.C(exports);
var __webpack_exec__ = (moduleId) => (__webpack_require__(__webpack_require__.s = moduleId))
var __webpack_exports__ = __webpack_require__.X(0, ["vendor-chunks/next"], () => (__webpack_exec__("(instrument)/./instrumentation.ts")));
module.exports = __webpack_exports__;

})();