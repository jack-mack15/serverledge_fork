function calculateFibonacci(n) {
    if (n <= 0) return 0;
    if (n === 1) return 1;

    let a = 0;
    let b = 1;

    for (let i = 2; i <= n; i++) {
        let temp = a + b;
        a = b;
        b = temp;
    }
    return b;
}

function handler(params, context) {
    let n = 10; // Valore di default

    if (params && params["n"] !== undefined) {
        n = parseInt(params["n"], 10);
    }

    return calculateFibonacci(n);
}

module.exports = handler;