const fs = require('fs');
const path = require('path');

console.log('🔧 Fixing column alignment between processor and database schemas...');

// Read the smart-file-processor.js file
const smartProcessorPath = path.join(__dirname, 'smart-file-processor.js');
let content = fs.readFileSync(smartProcessorPath, 'utf8');

// Fix 1: Cash Flow processor - align with table schema
const oldCashFlowMapping = `                    acquisition_date: this.parseDate(
                        record['ACQUISITION DATE'] || record['acquisition_date'] ||
                        record['Transaction Date'] || record['TransactionDate'] ||
                        new Date()
                    ),
                    settlement_date: this.parseDate(
                        record['SETTLEMENT DATE'] || record['settlement_date'] ||
                        record['Settlement Date'] || record['SettlementDate']
                    ),
                    amount: this.parseNumeric(
                        record['AMOUNT'] || record['amount'] ||
                        record['Amount']
                    ) || 0,
                    price: this.parseNumeric(
                        record['PRICE'] || record['price'] ||
                        record['Price']
                    ) || 0,
                    brokerage: this.parseNumeric(
                        record['BROKERAGE'] || record['brokerage'] ||
                        record['Brokerage']
                    ) || 0,
                    service_tax: this.parseNumeric(
                        record['SERVICE TAX'] || record['service_tax'] ||
                        record['Service Tax'] || record['ServiceTax']
                    ) || 0`;

const newCashFlowMapping = `                    transaction_date: this.parseDate(
                        record['ACQUISITION DATE'] || record['acquisition_date'] ||
                        record['Transaction Date'] || record['TransactionDate'] ||
                        new Date()
                    ),
                    settlement_date: this.parseDate(
                        record['SETTLEMENT DATE'] || record['settlement_date'] ||
                        record['Settlement Date'] || record['SettlementDate']
                    ),
                    amount: this.parseNumeric(
                        record['AMOUNT'] || record['amount'] ||
                        record['Amount']
                    ) || 0,
                    charges: this.parseNumeric(
                        record['BROKERAGE'] || record['brokerage'] ||
                        record['Brokerage'] || record['CHARGES'] || record['charges']
                    ) || 0,
                    tax: this.parseNumeric(
                        record['SERVICE TAX'] || record['service_tax'] ||
                        record['Service Tax'] || record['ServiceTax'] || record['TAX'] || record['tax']
                    ) || 0,
                    net_amount: this.parseNumeric(
                        record['NET AMOUNT'] || record['net_amount'] ||
                        record['Net Amount'] || record['PRICE'] || record['price']
                    ) || 0,
                    payment_mode: this.cleanValue(
                        record['PAYMENT MODE'] || record['payment_mode'] ||
                        record['Payment Mode'] || 'ONLINE'
                    ),
                    bank_reference: this.cleanValue(
                        record['BANK REFERENCE'] || record['bank_reference'] ||
                        record['Bank Reference'] || record['Reference']
                    )`;

// Apply fixes
if (content.includes("acquisition_date: this.parseDate(")) {
    content = content.replace(oldCashFlowMapping, newCashFlowMapping);
    console.log('✅ Fixed cash flow column mapping');
}

// Remove extra fields that don't exist in the table schema from the cash flow processor
const extraCashFlowFields = `,
                    settlement_date_flag: this.cleanValue(
                        record['SETTLEMENT DATE FLAG'] || record['settlement_date_flag'] ||
                        record['Settlement Date Flag']
                    ),
                    market_rate: this.parseNumeric(
                        record['MARKET RATE AS ON SECURITY IN DATE'] || record['market_rate'] ||
                        record['Market Rate']
                    ) || 0,
                    cash_symbol: this.cleanValue(
                        record['CASH SYMBOL'] || record['cash_symbol'] ||
                        record['Cash Symbol']
                    ),
                    stt_amount: this.parseNumeric(
                        record['STT AMOUNT'] || record['stt_amount'] ||
                        record['STT Amount'] || record['STT']
                    ) || 0,
                    accrued_interest: this.parseNumeric(
                        record['ACCRUED INTEREST'] || record['accrued_interest'] ||
                        record['Accrued Interest']
                    ) || 0,
                    block_ref: this.cleanValue(
                        record['BLOCK REF.'] || record['block_ref'] ||
                        record['Block Ref'] || record['Reference Number']
                    ),`;

// Remove these extra fields from cash flow processor
if (content.includes('settlement_date_flag: this.cleanValue(')) {
    content = content.replace(extraCashFlowFields, ',');
    console.log('✅ Removed extra fields from cash flow processor');
}

// Write back the fixed file
fs.writeFileSync(smartProcessorPath, content, 'utf8');

console.log('🎉 Column alignment fixed! Now the system will:');
console.log('   ✅ Use transaction_date instead of acquisition_date for cash flow');
console.log('   ✅ Use charges, tax, net_amount instead of price, brokerage, service_tax');
console.log('   ✅ Remove extra fields that don\'t exist in table schema');

console.log('\n🚀 Ready to process data with correct column alignment!'); 
const path = require('path');

console.log('🔧 Fixing column alignment between processor and database schemas...');

// Read the smart-file-processor.js file
const smartProcessorPath = path.join(__dirname, 'smart-file-processor.js');
let content = fs.readFileSync(smartProcessorPath, 'utf8');

// Fix 1: Cash Flow processor - align with table schema
const oldCashFlowMapping = `                    acquisition_date: this.parseDate(
                        record['ACQUISITION DATE'] || record['acquisition_date'] ||
                        record['Transaction Date'] || record['TransactionDate'] ||
                        new Date()
                    ),
                    settlement_date: this.parseDate(
                        record['SETTLEMENT DATE'] || record['settlement_date'] ||
                        record['Settlement Date'] || record['SettlementDate']
                    ),
                    amount: this.parseNumeric(
                        record['AMOUNT'] || record['amount'] ||
                        record['Amount']
                    ) || 0,
                    price: this.parseNumeric(
                        record['PRICE'] || record['price'] ||
                        record['Price']
                    ) || 0,
                    brokerage: this.parseNumeric(
                        record['BROKERAGE'] || record['brokerage'] ||
                        record['Brokerage']
                    ) || 0,
                    service_tax: this.parseNumeric(
                        record['SERVICE TAX'] || record['service_tax'] ||
                        record['Service Tax'] || record['ServiceTax']
                    ) || 0`;

const newCashFlowMapping = `                    transaction_date: this.parseDate(
                        record['ACQUISITION DATE'] || record['acquisition_date'] ||
                        record['Transaction Date'] || record['TransactionDate'] ||
                        new Date()
                    ),
                    settlement_date: this.parseDate(
                        record['SETTLEMENT DATE'] || record['settlement_date'] ||
                        record['Settlement Date'] || record['SettlementDate']
                    ),
                    amount: this.parseNumeric(
                        record['AMOUNT'] || record['amount'] ||
                        record['Amount']
                    ) || 0,
                    charges: this.parseNumeric(
                        record['BROKERAGE'] || record['brokerage'] ||
                        record['Brokerage'] || record['CHARGES'] || record['charges']
                    ) || 0,
                    tax: this.parseNumeric(
                        record['SERVICE TAX'] || record['service_tax'] ||
                        record['Service Tax'] || record['ServiceTax'] || record['TAX'] || record['tax']
                    ) || 0,
                    net_amount: this.parseNumeric(
                        record['NET AMOUNT'] || record['net_amount'] ||
                        record['Net Amount'] || record['PRICE'] || record['price']
                    ) || 0,
                    payment_mode: this.cleanValue(
                        record['PAYMENT MODE'] || record['payment_mode'] ||
                        record['Payment Mode'] || 'ONLINE'
                    ),
                    bank_reference: this.cleanValue(
                        record['BANK REFERENCE'] || record['bank_reference'] ||
                        record['Bank Reference'] || record['Reference']
                    )`;

// Apply fixes
if (content.includes("acquisition_date: this.parseDate(")) {
    content = content.replace(oldCashFlowMapping, newCashFlowMapping);
    console.log('✅ Fixed cash flow column mapping');
}

// Remove extra fields that don't exist in the table schema from the cash flow processor
const extraCashFlowFields = `,
                    settlement_date_flag: this.cleanValue(
                        record['SETTLEMENT DATE FLAG'] || record['settlement_date_flag'] ||
                        record['Settlement Date Flag']
                    ),
                    market_rate: this.parseNumeric(
                        record['MARKET RATE AS ON SECURITY IN DATE'] || record['market_rate'] ||
                        record['Market Rate']
                    ) || 0,
                    cash_symbol: this.cleanValue(
                        record['CASH SYMBOL'] || record['cash_symbol'] ||
                        record['Cash Symbol']
                    ),
                    stt_amount: this.parseNumeric(
                        record['STT AMOUNT'] || record['stt_amount'] ||
                        record['STT Amount'] || record['STT']
                    ) || 0,
                    accrued_interest: this.parseNumeric(
                        record['ACCRUED INTEREST'] || record['accrued_interest'] ||
                        record['Accrued Interest']
                    ) || 0,
                    block_ref: this.cleanValue(
                        record['BLOCK REF.'] || record['block_ref'] ||
                        record['Block Ref'] || record['Reference Number']
                    ),`;

// Remove these extra fields from cash flow processor
if (content.includes('settlement_date_flag: this.cleanValue(')) {
    content = content.replace(extraCashFlowFields, ',');
    console.log('✅ Removed extra fields from cash flow processor');
}

// Write back the fixed file
fs.writeFileSync(smartProcessorPath, content, 'utf8');

console.log('🎉 Column alignment fixed! Now the system will:');
console.log('   ✅ Use transaction_date instead of acquisition_date for cash flow');
console.log('   ✅ Use charges, tax, net_amount instead of price, brokerage, service_tax');
console.log('   ✅ Remove extra fields that don\'t exist in table schema');

console.log('\n🚀 Ready to process data with correct column alignment!'); 