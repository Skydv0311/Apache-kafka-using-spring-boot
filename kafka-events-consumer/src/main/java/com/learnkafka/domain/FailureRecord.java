package com.learnkafka.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;

@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
@Entity
public class FailureRecord {

    @Id
    @GeneratedValue
    private Integer id;
    private String topic;
    private Integer dkey;
    private String errorRecord;
    private Integer partition;
    private Long offset_value;
    private String exception;
    private String status;
}