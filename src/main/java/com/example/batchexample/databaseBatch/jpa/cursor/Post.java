package com.example.batchexample.databaseBatch.jpa.cursor;

import jakarta.persistence.*;
import lombok.Getter;
import org.hibernate.annotations.BatchSize;

import java.util.ArrayList;
import java.util.List;

/**
 * 게시글 엔티티 - 검열 대상
 */
@Entity
@Table(name = "posts")
@Getter
public class Post {
    @Id
    private Long id;
    private String title;         // 게시물 제목
    private String content;       // 게시물 내용
    private String writer;        // 작성자

    @OneToMany(mappedBy = "post", fetch = FetchType.EAGER) // FetchType EAGER 변경
    @BatchSize(size = 5) // @BatchSize 적용
    private List<Report> reports = new ArrayList<>();
}
