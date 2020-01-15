package com.skillbox.mongodemo;

import com.mongodb.BasicDBObject;
import com.mongodb.DocumentToDBRefTransformer;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.exceptions.CsvException;
import org.bson.Document;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

import static com.mongodb.client.model.Filters.*;

public class Test {

    public static void main(String[] args) {
        MongoClient mongoClient = new MongoClient("127.0.0.1", 27017);
        MongoDatabase database = mongoClient.getDatabase("local");
        MongoCollection<Document> collection = database.getCollection("Students");
        collection.drop();
        File file = new File("mongo.csv");

        List<String[]> lines = getLines(file);

        ArrayList<Document> docs = new ArrayList<>();
        for (String[] line : lines) {
            docs.add(new Document()
                    .append("Name", line[0])
                    .append("Age", Integer.valueOf(line[1]))
                    .append("Courses", Arrays.asList(line[2].split(",")))
            );
        }

        collection.insertMany(docs);

        // Количество записей в базе
        System.out.println("Общее количество студентов в базе = " + collection.countDocuments());

        // Количество студентов с возрастом больше 40
        ArrayList<Document> list = new ArrayList<>();
        BasicDBObject query = new BasicDBObject("Age", new BasicDBObject("$gt", 40));
        FindIterable findIterable = collection.find(query);
        findIterable.into(list);
        System.out.println("Количество студентов старше 40 лет = " + list.size());

        //Имя самого молодого студента
        collection.find()
                .sort(new Document("Age", 1))
                .limit(1)
                .forEach((Consumer<Document>) d  -> {
                    System.out.println("\"" + d.get("Name") + "\" самый молодой студент ему " + d.get("Age") + " лет");
                });


        //Список курсов самого старого студента
        collection.find()
                .sort(new Document("Age", -1)).limit(1).forEach((Consumer<Document>) d -> {
            System.out.println(d.get("Courses")  + " курсы по самого старого студента "
                    + d.get("Name") + ", ему " + d.get("Age"));
        });
    }


    private static List<String> getList(String courses){
        List<String> list = new ArrayList<>();

        return null;
    }
    private static List<String[]> getLines(File fileCsv){
        File file = fileCsv;

        try {
            FileReader fileReader = new FileReader(file);
            CSVReader csvReader = new CSVReaderBuilder(fileReader).build();
            List<String[]> lines = csvReader.readAll();
            return lines;

        } catch (CsvException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;
    }
}

/*
        MongoDatabase database = mongoClient.getDatabase("local");

        // Создаем коллекцию
        MongoCollection<Document> collection = database.getCollection("TestSkillDemo");

        // Удалим из нее все документы
        collection.drop();

        // Создадим первый документ
        Document firstDocument = new Document()
                .append("Type", 1)
                .append("Description", "Это наш первый документ в MongoDB")
                .append("Author", "Я")
                .append("Time", new SimpleDateFormat().format(new Date()));


        // Вложенный объект
        Document nestedObject = new Document()
                .append("Course", "NoSQL Базы Данных")
                .append("Author", "Mike Ovchinnikov");

        firstDocument.append("Skillbox", nestedObject);


        // Вставляем документ в коллекцию
        collection.insertOne(firstDocument);

        collection.find().forEach((Consumer<Document>) document -> {
            System.out.println("Наш первый документ:\n" + document);
        });

        // Используем JSON-синтаксис для создания объекта
        Document secondDocument = Document.parse(
                "{Type: 2, Description:\"Мы создали и нашли этот документ с помощью JSON-синтаксиса\"}"
        );
        collection.insertOne(secondDocument);

        // Используем JSON-синтаксис для написания запроса (выбираем документы с Type=2)
        BsonDocument query = BsonDocument.parse("{Type: {$eq: 2}}");
        collection.find(query).forEach((Consumer<Document>) document -> {
            System.out.println("Наш второй документ:\n" + document);
        });
        }
    }
}
*/
