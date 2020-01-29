import './polyfill';

export const gRows = Symbol('gRows');

interface Row {
    [k: string]: any
}
interface Values {
    [k: string]: any[]
}
interface AggFn {
    (df: DataFrame): [string, any]
}
interface RowFn {
    (df: DataFrame): any
}
interface Group {
    [gRows]: DataFrame;
    [k: string]: any;
}
interface NameMapping {
    [k: string]: string
}

// [ Row, Row, Row ]
export class DataFrame {
    rows: Row[];
    values: Values;

    constructor(rows: Row[]) {
        this.rows = rows;
        this.values = {};
    }

    get columns(): string[] {
        return this.rows.length ? Object.keys(this.rows[0]) : [];
    }

    groupBy(expr: string) {
        const exprFn = genExprFn(expr, this.columns);
        // { group1: [row, row, row],  group2: [row, row, row]}
        const g = this.rows.reduce((g: Values, row) => {
            const groupName = exprFn.apply(null, this.getRowValues(row))
            if(g[groupName]) {
                g[groupName].push(row);
            } else {
                g[groupName] = [row];
            }
            return g;
        }, {});
        // [ { expr: group1, [gRows]: df([row, row, row]) } 
        //   { expr: group2, [gRows]: df([row, row, row]) } ]
        const g2 = Object.entries(g).map(([colValue, rows])=>
            ({ [expr]: exprFn.apply(null, this.getRowValues(rows[0])), [gRows]: new DataFrame(rows) })
        );

        return new GroupDataFrame(this, g2);
    }

    select(expr: string) {
        const fn = parser(lexer(expr));

        const rows = this.rows.map(row => {
            return { [expr]: fn(row) }
        });

        return new DataFrame(rows);
    }

    agg(fn: AggFn) {
        return fn(this)[1];
    }

    // the order may need to reconsider.
    getValues(column: string) {
        if (!this.values[column]) {
            const set = new Set();
            this.rows.forEach(row => {
                if(row[column] != null) {
                    set.add(row[column])
                }
            });
            this.values[column] = Array.from(set);
        }
        return this.values[column];
    }

    getRowValues(row: Row) {
        return this.columns.map(colName => row[colName]);
    }

    // make sure data completion before rename
    rename(sMap: { [key: string]: string }, dMap?: (key: string) => string) {
        this.rows = this.rows.map((row: Row) => {
            const renamed: Row =  {};
            this.columns.forEach((key: string) => {
                if (sMap[key]) {
                    renamed[sMap[key]] = row[key];
                } else if (dMap) {
                    renamed[dMap(key)] = row[key];
                } else {
                    renamed[key] = row[key];
                }
            });
            return renamed;
        });
        return this;
    }

    getColumns(...except: string[]) {
        return this.columns.filter(column => !except.includes(column));
    }

    rowAgg(...fns: RowFn[]) {
        fns.forEach(fn => fn(this));
        return this;
    }

    columnAgg(nameCol: string, name: string, fns: AggFn[]) {
        const summary = fns.reduce((ret: Row, fn) => {
            const [column, value] = fn(this);
            ret[column] = value;
            return ret;
        }, {});
        this.rows.push({ [nameCol]: name, ...summary });
        return this;
    }
}



// [
//   { pId: 'P1', rows: DataFrame }
//   { pId: 'P2', rows: DataFrame }
// ]
export class GroupDataFrame {
    df: DataFrame;
    gData: Group[];

    constructor(dataframe: DataFrame, gData: Group[]) {
        this.df = dataframe;
        this.gData = gData;
    }

    // 因为不知道怎么处理嵌套groupby, 所以我把group的列拍平了.
    // return GroupDataFrame
    // [
    //   { pId: 'P1', country: 'US', rows: [Row] }
    //   { pId: 'P1', country: 'UK', rows: [Row, Row] }
    // ]
    groupBy(column: string) {
        const gData = this.gData.flatMap(gRow => {
            const { [gRows]: dataFrame, ...rest } = gRow;
            return dataFrame.groupBy(column).gData.map(gRow => ({ ...rest, ...gRow }));
        });
        return new GroupDataFrame(this.df, gData);
    }
    
    // pivot
    // 将columnToPivot这一列数据，变成新的列，数据聚合内容
    pivot(columnToPivot: string, aggFns: AggFn): DataFrame;
    pivot(columnToPivot: string, aggFns: AggFn[]): DataFrame[];
    pivot(columnToPivot: string, aggFns: AggFn | AggFn[]) {
        // 提前遍历获得所有新列名，保证行数据的完整性 test: groupDataframe pivot with uncompelete data
        // 提前遍历获得所有新列名，保证行数据的完整性 test: groupDataframe pivot with uncompelete data
        const newColumnNames = this.df.getValues(columnToPivot);

        if (!Array.isArray(aggFns)) {
            aggFns = [aggFns];
        }

        const datas = aggFns.map(aggFn => this._pivotAgg(columnToPivot, newColumnNames, aggFn));
        
        if (datas.length > 1) {
            return datas.map(data => new DataFrame(data));
        } else {
            return new DataFrame(datas[0]);
        }
    }

    // [{ date: '01', [gRows]: df } { date: '02', [gRows]: df }]
    // [{ date: '01', foo: 1, bar: 2 } { date: '02', foo: 3, bar: 4 }]
    private _pivotAgg(columnToPivot: string, newColumnNames: string[], aggFn: AggFn) {
        return this.gData.map(gRow => {
            const { [gRows]: df, ...groups } = gRow;
            // { name: 'foo',  [gRows]: } { name: 'bar',  [gRows]: }
            // { foo: aggFn(foo's rows), bar: aggFn(bar's rows)}
            const gdf = df.groupBy(columnToPivot);

            const newRowInit = newColumnNames.reduce((ret: Row, name) => {
                ret[name] = 0;
                return ret;
            }, {});

            const newRow = gdf.gData.reduce((ret: Row, { [gRows]: df, [columnToPivot]: newColumnName }) => {
                ret[newColumnName] = df.agg(aggFn);
                return ret;
            }, newRowInit);

            return {...newRow, ...groups };
        });
    }

    agg(...fns: AggFn[]) {
        const data = this.gData.map(gRow => {
            const { [gRows]: dataFrame, ...rest } = gRow;
            const ret = fns.reduce((ret: Row, fn) => {
                const [key, value] = fn(dataFrame);
                ret[key] = value;
                return ret;
            }, {});
            return { ...rest, ...ret };
        });
        return new DataFrame(data);
    }
}

export const aggFn = {
    sum: (column: string, alias?: string) => (df: DataFrame): [string, number] => {
        const { rows } = df;
        const value = rows.reduce((prev, row) => prev + (row[column] || 0), 0);
        return [alias || column, value];
    }
};

export const rowFn = {
    sum: (expect: string[], alias?: string) => (df: DataFrame) => {
        const { rows } = df;
        const key = alias || 'sum';
        const columns = df.getColumns(...expect);
        rows.forEach(row => {
            row[key] = columns.reduce((sum, col) => {
                return sum + row[col] || 0;
            }, 0);
        })
    }
};

interface Token {
    type: string,
    value: any
}

function lexer(expr: string): Token[] {
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

function parser(tokens: Token[]) {
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

export function genExprFn(expr: string, keys: string[]) {
    return new Function(`return function (${keys.join(',')}) { return ${expr}}`)();
}

export function toLineChart(df: DataFrame, categoryKey: string) {
    const legends = df.getColumns(categoryKey);
    return {
        legends,
        categories: df.rows.map(r => r[categoryKey]),
        series: legends.reduce((ret: any, legend) => {
            ret[legend] = df.rows.map(r => r[legend])
            return ret;
        }, {})
    }
}

// data [{ name: xx, value: 123}, { name: xx, value: 123}]
export function toPieChart(df: DataFrame, nameCol: string, valueCol: string) {
    return df.rows.map(row => ({ name: row[nameCol], value: row[valueCol] }));
}
