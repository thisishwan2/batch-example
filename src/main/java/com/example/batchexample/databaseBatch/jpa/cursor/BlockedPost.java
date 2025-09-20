package com.example.batchexample.databaseBatch.jpa.cursor;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Builder;
import lombok.Getter;

import java.time.LocalDateTime;

@Entity
@Table(name = "blocked_posts")
@Getter
@Builder
public class BlockedPost {
    @Id
    @Column(name = "post_id")
    private Long postId;

    private String writer;
    private String title;

    @Column(name = "report_count")
    private int reportCount;

    @Column(name = "block_score")
    private double blockScore;

    @Column(name = "blocked_at")
    private LocalDateTime blockedAt;
}
