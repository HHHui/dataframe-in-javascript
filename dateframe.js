const data = [
    {pId: 'P1', pName: 'PenDrive', quantity: 50,  country: 'US', gRows: 'US'},
    {pId: 'P1', pName: 'PenDrive', quantity: 100, country: 'UK', gRows: 'UK'},
    {pId: 'P2', pName: 'Mouse',    quantity: 100, country: 'UK', gRows: 'UK'},
    {pId: 'P3', pName: 'KeyBoard', quantity: 250, country: 'US', gRows: 'US'},
    {pId: 'P1', pName: 'PenDrive', quantity: 300, country: 'US', gRows: 'US'},
    {pId: 'P2', pName: 'Mouse',    quantity: 450, country: 'UK', gRows: 'UK'},
    {pId: 'P5', pName: 'Dvd',      quantity: 50,  country: 'UAE', gRows: 'UAE'}
];

const target = [
    { pName: 'Mouse', UK: 550, US: 0 }
];

const gRows = Symbol('gRows');

// [ Row, Row, Row ]
class DataFrame {
    constructor(data) {
        this.data = data;
    }

    groupBy(column) {
        const g = this.data.reduce((g, row) => {
            if(g[row[column]]) {
                g[row[column]].push(row);
            } else {
                g[row[column]] = [row];
            }
            return g;
        }, {});

        const g2 = Object.entries(g).map(([colValue, rows])=>
            ({ [column]: rows[0][column], [gRows]: rows })
        );

        return new GroupDataFrame(g2);
    }

    groupBys(columns) {
        for (let column of columns) {
            const g = this.groupBy(column);
        }
    }

    select(expr) {
        const fn = parser(lexer(expr));

        const data = this.data.map(row => {
            return { [expr]: fn(row) }
        });

        return new DataFrame(data);
    }
}

// [
//   { pId: 'P1', rows: [Row, Row, Row] }
//   { pId: 'P2', rows: [Row, Row, Row] }
// ]
class GroupDataFrame {
    constructor(gData) {
        this.gData = gData;
    }

    // return GroupDataFrame
    // [
    //   { pId: 'P1', country: 'US', rows: [Row] }
    //   { pId: 'P1', country: 'UK', rows: [Row, Row] }
    // ]
    groupBy(column) {
        const gData = this.gData.flatMap(gRow => {
            const { [gRows]: rows, ...rest } = gRow;
            return new DataFrame(rows).groupBy(column).gData.map(gRow => ({ ...rest, ...gRow }));
        });
        return new GroupDataFrame(gData);
    }

    agg(fn) {
        const data = this.gData.map(gRow => {
            const { [gRows]: rows, ...rest } = gRow;
            return { ...rest, ...fn(rows) }
        });
        return new DataFrame(data);
    }
}

const aggFn = {
    sum: (column) => (rows) => {
        const newColumn = `sum(${column})`;
        return rows.reduce((ret, row) => {
            if(!ret[newColumn]){
                ret[newColumn] = 0;
            }
            ret[newColumn] = ret[newColumn] + row[column];
            return ret;
        }, {});
    }
};

// console.log(new DataFrame(data)
//     .groupBy('pId')
//     .groupBy('country')
//     .agg(aggFn.sum('quantity')));

function lexer(expr) {
    const tokens = [];
    for (let word of expr.split(/\s+/)) {
        if(word.match(/^[a-zA-Z][\w]+$/) /* [a-zA-Z0-9_] */) {
            tokens.push({ type: 'identifier', value: word })
        } else if(word.match(/[\+\-\*\/]/) /* + * - / */) {
            tokens.push({ type: 'operator', value: word})
        } else if(word.match(/\d+/) /* only support integer TODO support double*/) {
            tokens.push({ type: 'literal', value: word})
        }
    }
    return tokens;
}

function parser(tokens) {
    let str = "return ";
    for (let {type, value} of tokens) {
        switch (type) {
            case 'identifier': {
                str += `row['${value}'] `;
                break;
            }
            default: {
                str += `${value} `
            }
        }
    }
    return new Function('row', str);
}

// console.log(
//     parser(lexer('quantity + 1'))
// )

console.log(
    new DataFrame(data).select('0 / 0', 'quantity')
);

// quantity + 1 这个是一个Expression -> row