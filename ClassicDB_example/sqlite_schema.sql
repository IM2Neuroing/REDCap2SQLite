CREATE TABLE `demographics`(
    `id` INTEGER PRIMARY KEY AUTOINCREMENT,
    `race` TEXT,
    `ethnicity` TEXT,
    `gender` TEXT,
    `dob` TEXT NOT NULL,
    UNIQUE(`race`, `ethnicity`, `gender`, `dob`)
);

CREATE TABLE `patients`(
    `id` INTEGER PRIMARY KEY AUTOINCREMENT,
    `sign_date` DATE NOT NULL,
    `first_name` TEXT NOT NULL,
    `last_name` TEXT NOT NULL,
    `email` TEXT,
    `demographics_id` INTEGER NOT NULL,
    FOREIGN KEY(`demographics_id`) REFERENCES `demographics`(`id`),
    UNIQUE(`first_name`, `last_name`, `email`)
);

CREATE TABLE `hospitalizations`(
    `id` INTEGER PRIMARY KEY AUTOINCREMENT,
    `patient_id` INTEGER NOT NULL,
    `cause` TEXT NOT NULL,
    `data_admission` DATE,
    `date_discharge` DATE,
    FOREIGN KEY(`patient_id`) REFERENCES `patients`(`id`),
    UNIQUE(`patient_id`, `cause`, `data_admission`, `date_discharge`)
);

CREATE TABLE `compliance`(
    `id` INTEGER PRIMARY KEY AUTOINCREMENT,
    `missed_treatments` INTEGER,
    `supplement_drinking` INTEGER,
    UNIQUE(`missed_treatments`, `supplement_drinking`)
);

CREATE TABLE `blood_levels`(
    `id` INTEGER PRIMARY KEY AUTOINCREMENT,
    `serum_albumin` INTEGER,
    `serum_prealbumin` INTEGER,
    `creatinine` INTEGER,
    `normalized_protein_catobolic_rate` INTEGER,
    `cholesterol` INTEGER,
    UNIQUE(`serum_albumin`, `serum_prealbumin`, `creatinine`, `normalized_protein_catobolic_rate`, `cholesterol`)
);

CREATE TABLE `visits`(
    `id` INTEGER PRIMARY KEY AUTOINCREMENT,
    `date` DATE NOT NULL,
    `patient_id` INTEGER NOT NULL,
    `blood_level_id` INTEGER NOT NULL,
    `compliance_id` INTEGER,
    FOREIGN KEY(`patient_id`) REFERENCES `patients`(`id`),
    FOREIGN KEY(`blood_level_id`) REFERENCES `blood_levels`(`id`),
    FOREIGN KEY(`compliance_id`) REFERENCES `compliance`(`id`),
    UNIQUE(`date`, `patient_id`, `blood_level_id`, `compliance_id`)
);
