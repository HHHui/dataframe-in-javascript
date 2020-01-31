import {DataFrame, GroupDataFrame, gRows, aggFn, rowFn, toLineChart, toPieChart, col} from "./dataframe";


const data = [
    { date: '2020-01-01', name: 'foo', value: 1 },
    { date: '2020-01-01', name: 'bar', value: 2 },
    { date: '2020-01-02', name: 'foo', value: 3 },
    { date: '2020-01-02', name: 'bar', value: 4 }
]

test('dataframe groupBy', () => {
    const gdf = new DataFrame(data).groupBy('date');

    expect(gdf).toBeInstanceOf(GroupDataFrame);

    const { [gRows]: df, ...groups } = gdf.gData[0];
    expect(groups).toStrictEqual({ date: '2020-01-01' });
    expect(df).toBeInstanceOf(DataFrame);
    expect(df.rows).toStrictEqual([
        { date: '2020-01-01', name: 'foo', value: 1 },
        { date: '2020-01-01', name: 'bar', value: 2 }
    ]);
});

test('dataframe groupBys', () => {
    const data = [
        { date: '2020-01-01', name: 'foo', value: 1 },
        { date: '2020-01-01', name: 'foo', value: 1 },
        { date: '2020-01-01', name: 'bar', value: 2 }
    ]

    const df = new DataFrame(data).groupBys(['date', 'name']).agg(aggFn.sum('value'));

    expect(df.rows).toStrictEqual([
        { date: '2020-01-01', name: 'foo', value: 2 },
        { date: '2020-01-01', name: 'bar', value: 2 }
    ]);
});

test('dataframe groupBy expr', () => {
    const data = [
        { date: '2020-01-01 00', value: 1 },
        { date: '2020-01-01 01', value: 2 },
        { date: '2020-01-02 00', value: 3 },
        { date: '2020-01-02 01', value: 4 }
    ]

    const df = new DataFrame(data).groupBy('date.substr(0, 10)').agg(aggFn.sum('value'));

    expect(df.rows).toStrictEqual([
        { 'date.substr(0, 10)': '2020-01-01', 'value': 3 },
        { 'date.substr(0, 10)': '2020-01-02', 'value': 7 }
    ]);
});

test('dataframe rename', () => {
    const data = [
        { date: '2020-01-01 00', value: 1, other: 'foo' },
        { date: '2020-01-01 01', value: 2, other: 'foo' },
        { date: '2020-01-02 00', value: 3, other: 'foo' },
        { date: '2020-01-02 01', value: 4, other: 'foo' }
    ]

    const df = new DataFrame(data)
        .groupBy('date.substr(0, 10)')
        .agg(aggFn.sum('value'))
        .rename({'date.substr(0, 10)': 'date'}, (key) => `${key}s`);

    expect(df.rows).toStrictEqual([
        { 'date': '2020-01-01', 'values': 3 },
        { 'date': '2020-01-02', 'values': 7 }
    ]);
});

test('dataframe getValues', () => {
    const data = [
        { date: '2020-01-01', name: 'foo' },
        { date: '2020-01-01', name: undefined },
        { date: '2020-01-01', name: null },
        { date: '2020-01-02', name: 'bar' }
    ]
    const df = new DataFrame(data);
    const values = df.getValues('name');
    expect(new Set(values)).toEqual(new Set(['foo', 'bar']));
    
    expect(values).toBe(df.values['name'])
})

test('dataframe rowAgg sum', () => {
    const data = [
        { date: '2020',a: 1, b: 2, c: 3 },
        { date: '2021',a: 3, b: 4, c: 5 },
    ]
    const df = new DataFrame(data).rowAgg(rowFn.sum(['date'], col().as('total')));
    expect(df.rows).toStrictEqual([
        { date: '2020',a: 1, b: 2, c: 3, total: 6 },
        { date: '2021',a: 3, b: 4, c: 5, total: 12 },
    ]);
});

test('dataframe rowAgg percent', () => {
    const data = [
        { date: '2020',a: 1, b: 2, c: 3 },
        { date: '2021',a: 3, b: 4, c: 5 },
    ]
    const df = new DataFrame(data).rowAgg(rowFn.percent(col('a').as('aPercent')));
    expect(df.rows).toStrictEqual([
        { date: '2020',a: 1, b: 2, c: 3, aPercent: "25.00%" },
        { date: '2021',a: 3, b: 4, c: 5, aPercent: "75.00%" },
    ]);
});

test('dataframe withColumns', () => {
    const data = [
        { date: '2020',a: 1, b: 2 },
        { date: '2021',a: 3, b: 4 },
    ]
    const df = new DataFrame(data).withColumn('a/b', 'a/b');
    expect(df.rows).toStrictEqual([
        { date: '2020',a: 1, b: 2, 'a/b': 0.5 },
        { date: '2021',a: 3, b: 4, 'a/b': 0.75 },
    ]);
});

test('dataframe colAgg', () => {
    const data = [
        { date: '2020',a: 1, b: 2, c: 3 },
        { date: '2021',a: 3, b: 4, c: 5 },
    ]
    const df = new DataFrame(data).columnAgg('date', 'total' , [aggFn.sum('a'), aggFn.sum('b')]);
    expect(df.rows).toStrictEqual([
        { date: '2020',a: 1, b: 2, c: 3 },
        { date: '2021',a: 3, b: 4, c: 5 },
        { date: 'total', a: 4, b: 6 }
    ]);
});

test('dataframe orderBy', () => {
    const data = [
        { a: '2', b: 2 },
        { a: '2', b: 1 },
        { a: '5', b: 1 },
        { a: '3', b: 1 },
        { a: '7', b: 1 }
    ];
    const df = new DataFrame(data)
        .orderBy(['a', 'b'], ['desc', 'asc']);
    expect(df.rows).toStrictEqual([
        { a: '7', b: 1 },
        { a: '5', b: 1 },
        { a: '3', b: 1 },
        { a: '2', b: 1 },
        { a: '2', b: 2 },
    ]);
});

test('toChartLine', () => {
    const data = [
        { date: '2020',a: 1, b: 2, c: 3 },
        { date: '2021',a: 3, b: 4, c: 5 },
    ]
    const chartData = toLineChart(new DataFrame(data), 'date');
    expect(chartData).toStrictEqual({
        categories: ['2020', '2021'],
        legends: ['a', 'b', 'c'],
        series: {
            a: [1,3],
            b: [2,4],
            c: [3,5]
        }
    });

    const chartData2 = toLineChart(new DataFrame(data), 'date', ['b', 'c']);
    expect(chartData2).toStrictEqual({
        categories: ['2020', '2021'],
        legends: ['b', 'c'],
        series: {
            b: [2,4],
            c: [3,5]
        }
    });

    const chartData3 = toLineChart(new DataFrame(data), 'date', { c: 'C', b: 'B', });
    expect(chartData3).toStrictEqual({
        categories: ['2020', '2021'],
        legends: ['C', 'B'],
        series: {
            B: [2,4],
            C: [3,5]
        }
    });
});

test('toPieChart', () => {
    const data = [
        { zone: 'a', count: 1},
        { zone: 'b', count: 2},
        { zone: 'c', count: 3}
    ];
    const chartData = toPieChart(new DataFrame(data), 'zone', 'count');
    expect(chartData).toStrictEqual([
        { name: 'a', value: 1},
        { name: 'b', value: 2},
        { name: 'c', value: 3}
    ]);
});

// select expr
test('dataframe select one cloumn', () => {
    const df = new DataFrame(data).select('name');
    expect(df.rows).toStrictEqual([
        { name: 'foo' },
        { name: 'bar' },
        { name: 'foo' },
        { name: 'bar' }
    ]);
})

test('dataframe select support one column +-*/', () => {
    const df = new DataFrame(data).select('value + 1');
    expect(df.rows).toStrictEqual([
        { 'value + 1': 2 },
        { 'value + 1': 3 },
        { 'value + 1': 4 },
        { 'value + 1': 5 }
    ]);
})
// select expr end

test('groupDataframe groupBy', () => {
    const gdf = new DataFrame(data).groupBy('date').groupBy('name');
    
    expect(gdf).toBeInstanceOf(GroupDataFrame);

    const { [gRows]: df, ...groups } = gdf.gData[0];
    expect(groups).toStrictEqual({ date: '2020-01-01', name: 'foo' });
    expect(df).toBeInstanceOf(DataFrame);
    expect(df.rows).toStrictEqual([
        { date: '2020-01-01', name: 'foo', value: 1 },
    ]);
});

test('groupDataframe pivot', () => {
    const data = [
        { date: '2020-01-01', name: 'foo', value: 1 },
        { date: '2020-01-01', name: 'foo', value: 2 },
        { date: '2020-01-01', name: 'bar', value: 3 },
        { date: '2020-01-01', name: 'bar', value: 4 }
    ]

    const df = new DataFrame(data).groupBy('date').pivot('name', aggFn.sum('value'))
    
    expect(df.rows).toStrictEqual([
        { date: '2020-01-01', foo: 3, bar: 7 },
    ])
})

test('groupDataframe pivot with uncompelete data', () => {

    const data = [
        { date: '2020-01-01', name: 'foo', value: 1 },
        { date: '2020-01-02', name: 'bar', value: 4 }
    ]

    const df = new DataFrame(data).groupBy('date').pivot('name', aggFn.sum('value'))
    
    expect(df.rows).toStrictEqual([
        { date: '2020-01-01', foo: 1, bar: 0 },
        { date: '2020-01-02', foo: 0, bar: 4 }
    ])
})

test('groupDataframe pivot with multiple aggregation function', () => {
    const data = [
        { date: '2020-01-01', name: 'foo', input: 1, output: 11 },
        { date: '2020-01-01', name: 'bar', input: 2, output: 12 },
        { date: '2020-01-02', name: 'foo', input: 3, output: 13 },
        { date: '2020-01-02', name: 'bar', input: 4, output: 14 },
        { date: '2020-01-02', name: 'bar', input: 6, output: 6 }
    ]

    const [inDf, outDf] = new DataFrame(data).groupBy('date').pivot('name', [aggFn.sum('input'), aggFn.sum('output')])
    
    expect(inDf.rows).toStrictEqual([
        { date: '2020-01-01', foo: 1, bar: 2 },
        { date: '2020-01-02', foo: 3, bar: 10 },
    ])
    expect(outDf.rows).toStrictEqual([
        { date: '2020-01-01', foo: 11, bar: 12 },
        { date: '2020-01-02', foo: 13, bar: 20 },
    ])
})

test('groupDataframe agg sum', () => {
    const df = new DataFrame(data).groupBy('date').agg(aggFn.sum('value'));
    
    expect(df).toBeInstanceOf(DataFrame);
    expect(df.rows).toStrictEqual([
        { date: '2020-01-01', 'value': 3 },
        { date: '2020-01-02', 'value': 7 }
    ])
})

test('groupDataframe agg sum with data has null value', () => {
    const data = [
        { date: '2020-01-01', value: 1 },
        { date: '2020-01-01' },
        { date: '2020-01-01', value: null },
        { date: '2020-01-02' },
    ]

    const df = new DataFrame(data).groupBy('date').agg(aggFn.sum('value'));
    
    expect(df).toBeInstanceOf(DataFrame);
    expect(df.rows).toStrictEqual([
        { date: '2020-01-01', 'value': 1 },
        { date: '2020-01-02', 'value': 0 }
    ])
})

test('groupDataframe should have its parent dataframe ref', () => {
    const df = new DataFrame(data);
    const gdf = df.groupBy('date');

    expect(gdf.df).toBe(df);
});
