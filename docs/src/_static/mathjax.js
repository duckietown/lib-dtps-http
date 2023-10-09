window.MathJax = {
    tex: {
        inlineMath: [["\\(", "\\)"]],
        displayMath: [["\\[", "\\]"]],
        processEscapes: true,
        processEnvironments: true,
        // packages: {'[+]': ['color']},
        macros: {
            // F: '{\\bf \\color{green} F}',
            // R: '{\\bf \\color{red} R}',
            // fun: '{\\color{green}f}',
            // res: '{\\color{red}r}',
            fun: '{f}',
            res: '{r}',
            F: '{\\bf F}',
            R: '{\\bf R}',
            common: '{\\bf C}',
            opspace: '{\\bf Q}',
            posA: '{\\bf P}',
            imp: '{i}',
            impspace: '{\\bf I}',
        }
    },
    options: {
        ignoreHtmlClass: ".*|",
        processHtmlClass: "arithmatex"
    }
};

document$.subscribe(() => {
    MathJax.typesetPromise()
})
