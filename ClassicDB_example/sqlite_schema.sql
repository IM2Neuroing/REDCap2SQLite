CREATE TABLE `demographics`(
    `id` INTEGER PRIMARY KEY AUTOINCREMENT,
    `race` TEXT,
    `ethnicity` TEXT,
    `gender` TEXT,
    `dob` TEXT NOT NULL
);

CREATE TABLE `patients`(
    `id` INTEGER NOT NULL,
    `sign_date` DATE NOT NULL,
    `first_name` TEXT NOT NULL,
    `last_name` TEXT NOT NULL,
    `email` TEXT,
    `demographics_id` INTEGER NOT NULL,
    PRIMARY KEY(`id`),
    FOREIGN KEY(`demographics_id`) REFERENCES `demographics`(`id`)
);

CREATE TABLE `hospitalizations`(
    `id` INTEGER PRIMARY KEY AUTOINCREMENT,
    `patient_id` INTEGER NOT NULL,
    `cause` TEXT NOT NULL,
    `data_admission` DATE,
    `date_discharge` DATE,
    FOREIGN KEY(`patient_id`) REFERENCES `patients`(`id`)
);

CREATE TABLE `compliance`(
    `id` INTEGER PRIMARY KEY AUTOINCREMENT,
    `missed_treatments` INTEGER,
    `supplement_drinking` INTEGER
);

CREATE TABLE `blood_levels`(
    `id` INTEGER PRIMARY KEY AUTOINCREMENT,
    `serum_albumin` INTEGER,
    `serum_prealbumin` INTEGER,
    `creatinine` INTEGER,
    `normalized_protein_catobolic_rate` INTEGER,
    `cholesterol` INTEGER
);

CREATE TABLE `visits`(
    `id` INTEGER PRIMARY KEY AUTOINCREMENT,
    `date` DATE NOT NULL,
    `patient_id` INTEGER NOT NULL,
    `blood_level_id` INTEGER NOT NULL,
    `compliance_id` INTEGER NOT NULL,
    FOREIGN KEY(`patient_id`) REFERENCES `patients`(`id`),
    FOREIGN KEY(`blood_level_id`) REFERENCES `blood_levels`(`id`),
    FOREIGN KEY(`compliance_id`) REFERENCES `compliance`(`id`)
);
