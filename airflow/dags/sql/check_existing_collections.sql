SELECT COUNT(*) as count
FROM mongodb.information_schema.tables 
WHERE table_schema = 'imcp' 
AND table_name IN('audit','raw','refined','featured');