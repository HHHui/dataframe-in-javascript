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
    { pName: 'Mouse', UK: 550, US: 0}
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
}

console.log(new DataFrame(data).groupBy('pId').groupBy('gRows'));