const flatMap = (f,xs) =>
  xs.map(f).reduce((x,y) => x.concat(y), [])

Array.prototype.flatMap = function(f) {
  return flatMap(f,this)
}


export const gRows = Symbol('gRows');

// [ Row, Row, Row ]
export class DataFrame {
    constructor(rows) {
        this.rows = rows;
    }

    groupBy(column) {
        const g = this.rows.reduce((g, row) => {
            if(g[row[column]]) {
                g[row[column]].push(row);
            } else {
                g[row[column]] = [row];
            }
            return g;
        }, {});

        const g2 = Object.entries(g).map(([colValue, rows])=>
            ({ [column]: rows[0][column], [gRows]: new DataFrame(rows) })
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

        const rows = this.rows.map(row => {
            return { [expr]: fn(row) }
        });

        return new DataFrame(rows);
    }

    agg(fn) {
        return fn(this)[1];
    }

    getValues(column) {
        const set = new Set();
        this.rows.forEach(row => {
            if(row[column] != null) {
                set.add(row[column])
            }
        });
        return Array.from(set);
    }
}

// [
//   { pId: 'P1', rows: DataFrame }
//   { pId: 'P2', rows: DataFrame }
// ]
export class GroupDataFrame {
    constructor(gData) {
        this.gData = gData;
    }

    // 因为不知道怎么处理嵌套groupby, 所以我把group的列拍平了.
    // return GroupDataFrame
    // [
    //   { pId: 'P1', country: 'US', rows: [Row] }
    //   { pId: 'P1', country: 'UK', rows: [Row, Row] }
    // ]
    groupBy(column) {
        const gData = this.gData.flatMap(gRow => {
            const { [gRows]: dataFrame, ...rest } = gRow;
            return dataFrame.groupBy(column).gData.map(gRow => ({ ...rest, ...gRow }));
        });
        return new GroupDataFrame(gData);
    }
    
    // pivot
    // 将columnToPivot这一列数据，变成新的列，数据聚合内容
    pivot(columnToPivot, aggFn) {
        // 提前遍历获得所有新列名，保证行数据的完整性 test: groupDataframe pivot with uncompelete data
        const newColumnNames = this.getValues(columnToPivot);
        
        const data = this.gData.map(gRow => {
            const { [gRows]: df, ...rest } = gRow;
            // { name: 'foo',  [gRows]: }
            // { name: 'bar',  [gRows]: }
            // to
            // { foo: aggFn(foo's rows), bar: aggFn(bar's rows)}
            const gdf = df.groupBy(columnToPivot);

            const newRowInit = newColumnNames.reduce((ret, name) => {
                ret[name] = 0;
                return ret;
            }, {});

            const newRow = gdf.gData.reduce((ret, { [columnToPivot]: newColumnName, [gRows]: df }) => {
                ret[newColumnName] = df.agg(aggFn);
                return ret;
            }, newRowInit);
            return {...newRow, ...rest };
        });

        return new DataFrame(data);
    }

    agg(fn) {
        const data = this.gData.map(gRow => {
            const { [gRows]: dataFrame, ...rest } = gRow;
            const [key, value] = fn(dataFrame);
            return { ...rest, [key]: value };
        });
        return new DataFrame(data);
    }

    getValues(column) {
        const values = this.gData.flatMap(gRow => gRow[gRows].getValues(column));
        return Array.from(new Set(values));
    }
}

export const aggFn = {
    sum: (column) => ({ rows }) => {
        const key = `sum(${column})`;
        const value = rows.reduce((prev, row) => prev + (row[column] || 0), 0);
        return [key, value];
    }
};

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
