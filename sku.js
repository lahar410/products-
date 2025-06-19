const fs = require('fs');
const csv = require('csv-parser');
const { writeToPath } = require('@fast-csv/format');

const inputFile = './sku.csv';

const sku1Set = new Set();
const sku2Set = new Set();

fs.createReadStream(inputFile)
  .pipe(csv())
  .on('data', (row) => {
    const s1 = row.sku1?.trim();
    const s2 = row.sku1?.trim();

    if (s1) {
      sku1Set.add(s1);
      console.log('sku1:', s1);
    }

    if (s2) {
      sku2Set.add(s2);
      console.log('sku2:', s2);
    }
  })
  .on('end', () => {
    const onlyInSku1 = [...sku1Set].filter((sku) => !sku2Set.has(sku));
    const onlyInSku2 = [...sku2Set].filter((sku) => !sku1Set.has(sku));
    const inBoth = [...sku1Set].filter((sku) => sku2Set.has(sku));

    console.log('\n✅ only in sku1:', onlyInSku1);
    console.log('✅ only in sku2:', onlyInSku2);
    console.log('✅ in both:', inBoth);

    writeToPath('only_in_sku1.csv', onlyInSku1.map(sku => ({ sku })), { headers: true })
      .on('finish', () => console.log('✅ only_in_sku1.csv written'));

    writeToPath('only_in_sku2.csv', onlyInSku2.map(sku => ({ sku })), { headers: true })
      .on('finish', () => console.log('✅ only_in_sku2.csv written'));

    writeToPath('in_both.csv', inBoth.map(sku => ({ sku })), { headers: true })
      .on('finish', () => console.log('✅ in_both.csv written'));
  })
  .on('error', (err) => {
    console.error('❌ Error reading file:', err);
  });
